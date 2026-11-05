/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro.runtime;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;

/// Runs a [Machine], knowing nothing of what it does. Under one lock, it posts each fact and polls;
/// once the lock is released, it hands the commands the poll returned to the [Interpreter], each on
/// the thread its resource calls for: a virtual thread for I/O, a fixed pool for CPU, or, with zero
/// threads, the consumer's thread inside [#await]. A command's result is posted as a fact in turn,
/// by the thread that ran it.
///
/// @param <F> the machine's facts
/// @param <C> the machine's commands
/// @param <M> the machine
public final class Driver<F, C, M extends Machine<F, C>> implements AutoCloseable {

    private final M machine;
    private final Interpreter<C, F> interpreter;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final ExecutorService pool;
    private final ExecutorService io = Executors.newVirtualThreadPerTaskExecutor();
    /// CPU commands a driver of zero threads runs on the consumer's thread
    private final ArrayDeque<C> local = new ArrayDeque<>();
    /// commands handed out whose fact has not been posted yet
    private int inFlight;
    private final boolean trace;

    /// @param threads the pool's threads; `0` runs the CPU commands on the consumer's thread
    /// @param trace whether to print every fact, the thread posting it, and what the poll returned
    public Driver(M machine, Interpreter<C, F> interpreter, int threads, boolean trace) {
        this.machine = machine;
        this.interpreter = interpreter;
        this.pool = threads > 0 ? Executors.newFixedThreadPool(threads) : null;
        this.trace = trace;
        // the first poll starts the machine
        postAndPoll(null);
    }

    /// Posts `fact`, unless `null`, and polls, under the lock; dispatches what the poll returned
    /// once the lock is released.
    private void postAndPoll(F fact) {
        List<C> commands;
        lock.lock();
        try {
            if (fact != null) {
                machine.post(fact);
                inFlight--;
            }
            commands = pollLocked();
            if (trace) {
                Thread t = Thread.currentThread();
                System.out.printf("%-16s %-14s -> %-50s | %s%n", t.isVirtual() ? "io (virtual)" : t.getName(),
                        fact == null ? "start" : fact, commands, machine);
            }
        }
        finally {
            lock.unlock();
        }
        dispatch(commands);
    }

    private List<C> pollLocked() {
        List<C> commands = machine.poll();
        inFlight += commands.size();
        changed.signalAll();
        return commands;
    }

    /// Sends each command to where its resource runs.
    private void dispatch(List<C> commands) {
        for (C command : commands) {
            switch (interpreter.resource(command)) {
                case IO -> io.execute(() -> run(command));
                case CPU -> {
                    if (pool != null) {
                        pool.execute(() -> run(command));
                    }
                    else {
                        lock.lock();
                        try {
                            local.add(command);
                            changed.signalAll();
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                }
            }
        }
    }

    /// Runs `command` outside the lock, then posts its result.
    private void run(C command) {
        F fact;
        try {
            fact = interpreter.execute(command);
        }
        catch (Exception e) {
            fact = interpreter.failed(command, e);
        }
        postAndPoll(fact);
    }

    /// Waits until `ready` holds for the machine, running the CPU commands of a driver of zero
    /// threads meanwhile; then applies `action` to the machine and polls, since an action, such as
    /// taking a result, changes the machine's state. Both run under the lock.
    public <T> T await(Predicate<? super M> ready, Function<? super M, T> action) {
        while (true) {
            C work = null;
            T result;
            List<C> commands;
            lock.lock();
            try {
                while (!ready.test(machine) && (work = local.poll()) == null) {
                    changed.awaitUninterruptibly();
                }
                if (work != null) {
                    result = null;
                    commands = List.of();
                }
                else {
                    result = action.apply(machine);
                    commands = pollLocked();
                }
            }
            finally {
                lock.unlock();
            }
            if (work == null) {
                dispatch(commands);
                return result;
            }
            run(work);
        }
    }

    /// Whether no command handed out is waiting for its fact.
    public boolean quiet() {
        lock.lock();
        try {
            return inFlight == 0;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.shutdownNow();
        }
        io.shutdownNow();
    }
}
