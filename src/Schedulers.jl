module Schedulers

using Distributed, DistributedOperations, Printf, Random, Statistics

epmap_default_addprocs = n->addprocs(n)
epmap_default_preempted = ()->false

function load_modules_on_new_workers(pid)
    _names = names(Main; imported=true)
    for _name in _names
        try
            if isa(Base.eval(Main, _name), Module) && _name ∉ (:Base, :Core, :InteractiveUtils, :VSCodeServer, :Main, :_vscodeserver)
                remotecall_fetch(Base.eval, pid, Main, :(using $_name))
            end
        catch e
            @debug "caught error in load_modules_on_new_workers"
        end
    end
    nothing
end

function load_functions_on_new_workers(pid)
    _names = names(Main; imported=true)
    for _name in _names
        try
            @sync if isa(Base.eval(Main, _name), Function)
                @async remotecall_fetch(Base.eval, pid, Main, :(function $_name end))
            end
            @sync for m in Base.eval(Main, :(methods($_name)))
                @async remotecall_fetch(Base.eval, pid, Main, :($m))
            end
        catch e
            if _name ∉ (Symbol("@enter"), Symbol("@run"), :ans, :vscodedisplay)
                @debug "caught error in load_functions_on_new_workers for function $_name"
            end
        end
    end
end

# for performance metrics, track when the pid is started
const _pid_up_timestamp = Dict{Int, Float64}()

function elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, tsk_count, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs)
    for worker in workers()
        _pid_up_timestamp[worker] = time()
        put!(pid_channel, worker)
    end

    while true
        @debug "checking pool, length=$(length(tsk_pool_done)), count=$tsk_count"
        yield()
        length(tsk_pool_done) == tsk_count && (put!(pid_channel, -1); break)
        @debug "checking for interrupt=$interrupted"
        yield()
        interrupted && break
        @debug "checking for workers, nworkers=$(nworkers()), max=$epmap_maxworkers, #todo=$(length(tsk_pool_todo))"
        yield()
        n = min(epmap_maxworkers-nworkers(), epmap_quantum, length(tsk_pool_todo))
        if n > 0
            new_pids = epmap_addprocs(n)
            for new_pid in new_pids
                load_modules_on_new_workers(new_pid)
                load_functions_on_new_workers(new_pid)
            end
            for new_pid in new_pids
                _pid_up_timestamp[new_pid] = time()
                put!(pid_channel, new_pid)
            end
        end

        @debug "checking for workers to remove"
        while isready(rm_pid_channel)
            pid = take!(rm_pid_channel)
            _nworkers = 1 ∈ workers() ? nworkers()-1 : nworkers()
            @debug "removing worker $pid"
            if _nworkers > epmap_minworkers
                rmprocs(pid)
            end
        end

        sleep(5)
    end
    nothing
end

"""
    epmap(f, tasks, args...; pmap_kwargs..., f_kwargs...)

where `f` is the map function, and `tasks` is an iterable collection of tasks.  The function `f`
takes the positional arguments `args`, and the keyword arguments `f_args`.  The optional arguments
`pmap_kwargs` are as follows.

## pmap_kwargs
* `epmap_retries=0` number of times to retry a task on a given machine before removing that machine from the cluster
* `epmap_maxerrors=Inf` the maximum number of errors before we give-up and exit
* `epmap_minworkers=nworkers()` the minimum number of workers to elastically shrink to
* `epmap_maxworkers=nworkers()` the maximum number of workers to elastically expand to
* `epmap_quantum=32` the maximum number of workers to elastically add at a time
* `epmap_addprocs=n->addprocs(n)` method for adding n processes (will depend on the cluster manager being used)
* `epmap_preempted=()->false` method for determining of a machine got pre-empted (removed on purpose)[1]

## Notes
[1] For example, on Azure Cloud a SPOT instance will be pre-empted if someone is willing to pay more for it
"""
function epmap(f::Function, tasks, args...;
        epmap_retries = 0,
        epmap_maxerrors = Inf,
        epmap_minworkers = nworkers(),
        epmap_maxworkers = nworkers(),
        epmap_quantum = 32,
        epmap_addprocs = epmap_default_addprocs,
        epmap_preempted = epmap_default_preempted,
        kwargs...)
    tsk_pool_todo = collect(tasks)
    tsk_pool_done = []
    tsk_count = length(tsk_pool_todo)

    pid_channel = Channel{Int}(32)
    rm_pid_channel = Channel{Int}(32)

    fails = Dict{Int,Int}()
    map(pid->fails[pid]=0, workers())

    interrupted = false

    _elastic_loop = @async elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, tsk_count, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs)

    # work loop
    @sync while true
        interrupted && break
        pid = take!(pid_channel)
        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.
        @async while true
            interrupted && break
            try
                if remotecall_fetch(epmap_preempted, pid)
                    rmprocs(pid)
                    break
                end
            catch e
                @debug "unable to call preempted method"
            end

            isempty(tsk_pool_todo) && (put!(rm_pid_channel, pid); break)
            length(tsk_pool_done) == tsk_count && break
            isempty(tsk_pool_todo) && (yield(); continue)

            local tsk
            try
                tsk = popfirst!(tsk_pool_todo)
            catch
                # just in case another green-thread does popfirst! before us (unlikely)
                yield()
                continue
            end

            try
                @info "running task $tsk on process $pid; $(nworkers()) workers total; $(length(tsk_pool_todo)) tasks left in task-pool."
                yield()
                remotecall_fetch(f, pid, tsk, args...; kwargs...)
                @debug "...pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$tsk_pool_todo -!"
                yield()
                push!(tsk_pool_done, tsk)
            catch e
                fails[pid] += 1
                nerrors = sum(values(fails))
                @warn "caught an exception, there have been $(fails[pid]) failure(s) on process $pid..."
                showerror(stderr, e)
                push!(tsk_pool_todo, tsk)
                if isa(e, InterruptException)
                    interrupted = true
                    put!(pid_channel, -1)
                    throw(e)
                elseif isa(e, ProcessExitedException)
                    @warn "process with id=$pid exited, removing from process list"
                    rmprocs(pid)
                    break
                elseif nerrors >= epmap_maxerrors
                    interrupted = true
                    put!(pid_channel, -1)
                    error("too many errors, $nerrors errors")
                elseif fails[pid] > epmap_retries
                    @warn "too many failures on process with id=$pid, removing from process list"
                    rmprocs(pid)
                    break
                end
            end
        end
    end
    fetch(_elastic_loop)

    # ensure we are left with epmap_minworkers
    _workers = workers()
    1 ∈ _workers && popfirst!(_workers)
    rmprocs(_workers[1:(length(_workers) - epmap_minworkers)])

    nothing
end

function epmapreduce!(result::AbstractArray{T,N}, f, tasks, f_args...; f_kwargs...) where {T}
    verbosity > 0 && @info "creating pools..."
    pools = partition(collect(tasks), poolsize)

    copier = DistributedOperations.paralleloperations_copy!
    reducer = DistributedOperations.paralleloperations_reduce!
    filler = DistributedOperations.paralleloperations_fill!

    maxerrors = Inf

    @info "creating Futures..."
    futures = ArrayFutures(result, procs())
    futures_copy = ArrayFutures(result, procs())
    @info "...done creating Futures."

    fails = Dict{Int,Int}()
    map(pid->fails[pid]=0, workers())

    while true
        isempty(pools) && break
        isexception = false
        _pool = popfirst!(pools)
        pool = copy(_pool)
        try
            N = length(pools) == 0 ? 0 : mapreduce(length, +, pools)
            @sync for pid in workers()
                @async while true
                    isempty(pool) && break
                    tsk = popfirst!(pool)
                    try
                        verbosity > 0 && @info "Scheduling task $tsk ($(N+length(pool)) remaining tasks)"
                        remotecall_fetch(f, pid, tsk, futures, f_args...; f_kwargs...)
                    catch e
                        fails[pid] += 1
                        @warn "caught exception, $(fails[pid]) failures on process $(pid)..."
                        showerror(stderr, e)
                        if fails[pid] > retries
                            @warn "too many failures on process $(pid), removing from process list"
                            rmprocs([pid])
                        end
                        throw(e)
                    end
                end
            end
        catch e
            isexception = true
            nerrors = sum(values(fails))
            if nerrors >= maxerrors
                error("Too many errors ($(nerrors)) thrown, maximum allowed number of errors is $(maxerrors).")
            end
            push!(pools, _pool)
            futures_copy = ArrayFutures(result, procs())
            copy!(futures_copy, futures, copier, [1])
            futures = ArrayFutures(result, procs())
            copy!(futures, futures_copy, copier, [1])
        end

        if !isexception
            if isempty(pools)
                reduce!(futures, reducer)
            else
                copy!(futures_copy, futures, copier)
                reduce!(futures_copy, reducer)
                copy!(futures, futures_copy, copier, [1])
                nprocs() > nworkers() && fill!(futures, 0, filler, workers())
            end
        end
    end
    x
end

function partition(pool, N)
    pools = Vector{Int}[]
    last = 0
    while true
        frst = last + 1
        last = min(frst+N-1,length(pool))
        push!(pools, pool[frst:last])
        last == length(pool) && break
    end
    pools
end

const _timers = Dict{String,Dict{Int,Dict{String,Float64}}}()

function timers_analysis_map(filename)
    pids = collect(keys(Schedulers._timers["map"]))

    f = [Schedulers._timers["map"][pid]["f"] for pid in pids]
    μ_f = mean(f)
    σ_f = sqrt(var(f))

    checkpoint = [Schedulers._timers["map"][pid]["checkpoint"] for pid in pids]
    μ_checkpoint = mean(checkpoint)
    σ_checkpoint = sqrt(var(checkpoint))

    restart = [Schedulers._timers["map"][pid]["restart"] for pid in pids]
    μ_restart = mean(restart)
    σ_restart = sqrt(var(restart))

    cumulative = [Schedulers._timers["map"][pid]["cumulative"] for pid in pids]
    μ_cumulative = mean(cumulative)
    σ_cumulative = sqrt(var(cumulative))

    uptime = [Schedulers._timers["map"][pid]["uptime"] for pid in pids]
    μ_uptime = mean(uptime)
    σ_uptime = sqrt(var(uptime))

    utilization = (f .+ checkpoint .+ restart) ./ cumulative
    μ_utilization = mean(utilization)
    σ_utilization = sqrt(var(utilization))

    utilization_f = f ./ cumulative
    μ_utilization_f = mean(utilization_f)
    σ_utilization_f = sqrt(var(utilization_f))

    x = """
    | pid      | f    | checkpoint    | restart    | cumulative    | uptime         | utilization   | utilization_f    | 
    |----------|------|---------------|------------|---------------|----------------|---------------|------------------|
    """
    for (ipid,pid) in enumerate(pids)
        x *= """
        | $pid | $(@sprintf("%.2f",f[ipid])) | $(@sprintf("%.2f",checkpoint[ipid])) | $(@sprintf("%.2f",restart[ipid])) | $(@sprintf("%.2f",cumulative[ipid])) | $(@sprintf("%.2f",uptime[ipid])) | $(@sprintf("%.2f",utilization[ipid])) | $(@sprintf("%.2f",utilization_f[ipid])) |
        """
    end
    x *= """
    | **mean**     | $(@sprintf("%.2f",μ_f)) | $(@sprintf("%.2f",μ_checkpoint)) | $(@sprintf("%.2f",μ_restart)) | $(@sprintf("%.2f",μ_cumulative)) | $(@sprintf("%.2f",μ_uptime)) | $(@sprintf("%.2f",μ_utilization)) | $(@sprintf("%.2f",μ_utilization_f)) |
    | **variance** | $(@sprintf("%.2f",σ_f)) | $(@sprintf("%.2f",σ_checkpoint)) | $(@sprintf("%.2f",σ_restart)) | $(@sprintf("%.2f",σ_cumulative)) | $(@sprintf("%.2f",σ_uptime)) | $(@sprintf("%.2f",σ_utilization)) | $(@sprintf("%.2f",σ_utilization_f)) |
    """
    write(filename, x)
end

function timers_analysis_reduce(filename)
    pids = collect(keys(Schedulers._timers["reduce"]))

    reduce = [Schedulers._timers["reduce"][pid]["reduce"] for pid in pids]
    μ_reduce = mean(reduce)
    σ_reduce = sqrt(var(reduce))

    io = [Schedulers._timers["reduce"][pid]["IO"] for pid in pids]
    μ_io = mean(io)
    σ_io = sqrt(var(io))

    cleanup = [Schedulers._timers["reduce"][pid]["cleanup"] for pid in pids]
    μ_cleanup = mean(cleanup)
    σ_cleanup = sqrt(var(cleanup))

    cumulative = [Schedulers._timers["reduce"][pid]["cumulative"] for pid in pids]
    μ_cumulative = mean(cumulative)
    σ_cumulative = sqrt(var(cumulative))

    uptime = [Schedulers._timers["reduce"][pid]["uptime"] for pid in pids]
    μ_uptime = mean(uptime)
    σ_uptime = sqrt(var(uptime))

    utilization = (reduce .+ io .+ cleanup) ./ cumulative
    μ_utilization = mean(utilization)
    σ_utilization = sqrt(var(utilization))

    utilization_reduce = reduce ./ cumulative
    μ_utilization_reduce = mean(utilization_reduce)
    σ_utilization_reduce = sqrt(var(utilization_reduce))

    x = """
    | pid      | reduce | IO | cleanup | cumulative | uptime | utilization   | utilization_reduce |
    |----------|--------|----|---------|------------|--------|---------------|--------------------|
    """
    for (ipid,pid) in enumerate(pids)
        x *= """
        | $pid | $(@sprintf("%.2f",reduce[ipid])) | $(@sprintf("%.2f",io[ipid])) | $(@sprintf("%.2f",cleanup[ipid])) | $(@sprintf("%.2f",cumulative[ipid])) | $(@sprintf("%.2f",uptime[ipid])) | $(@sprintf("%.2f",utilization[ipid])) | $(@sprintf("%.2f",utilization_reduce[ipid])) |
        """
    end
    x *= """
    | **mean**     | $(@sprintf("%.2f",μ_reduce)) | $(@sprintf("%.2f",μ_io)) | $(@sprintf("%.2f",μ_cleanup)) | $(@sprintf("%.2f",μ_cumulative)) | $(@sprintf("%.2f",μ_uptime)) | $(@sprintf("%.2f",μ_utilization)) | $(@sprintf("%.2f",μ_utilization_reduce)) |
    | **variance** | $(@sprintf("%.2f",σ_reduce)) | $(@sprintf("%.2f",σ_io)) | $(@sprintf("%.2f",σ_cleanup)) | $(@sprintf("%.2f",σ_cumulative)) | $(@sprintf("%.2f",σ_uptime)) | $(@sprintf("%.2f",σ_utilization)) | $(@sprintf("%.2f",σ_utilization_reduce)) |
    """
    write(filename, x)
end

let CID::Int = 1
    global next_checkpoint_id
    next_checkpoint_id() = (id = CID; CID += 1; id)
end
next_checkpoint(id, scratch) = joinpath(scratch, string("checkpoint-", id, "-", next_checkpoint_id()))

function reduce(checkpoint1, checkpoint2, checkpoint3, ::Type{T}, n::NTuple{N,Int}) where {T,N}
    t_io = @elapsed begin
        c1 = read!(checkpoint1, Array{T,N}(undef, n))
        c2 = read!(checkpoint2, Array{T,N}(undef, n))
    end
    t_sum = @elapsed begin
        c3 = c1 .+ c2
    end
    t_io += @elapsed write(checkpoint3, c3)
    t_io, t_sum
end

save_checkpoint(checkpoint, localresult, ::Type{T}, n::NTuple{N,Int}) where {T,N} = (write(checkpoint, fetch(localresult)::Array{T,N}); nothing)
restart(orphan, localresult, ::Type{T}, n::NTuple{N,Int}) where {T,N} = (fetch(localresult)::Array{T,N} .+= read!(orphan, Array{T,N}(undef, n)); nothing)
rm_checkpoint(checkpoint) = isfile(checkpoint) && rm(checkpoint)

function handle_process_exited(pid, localresults, checkpoints, orphans)
    rmprocs(pid)
    pop!(localresults, pid)
    checkpoint = pop!(checkpoints, pid)
    checkpoint == nothing || push!(orphans, checkpoint)
    nothing
end

export epmap, epmapreduce!

end
