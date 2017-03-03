/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package core.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import core.utils.PriorityProvider;
import core.utils.PriorityTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTracingAwareExecutorService implements TracingAwareExecutorService
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractTracingAwareExecutorService.class);

    protected abstract void addTask(FutureTask<?> futureTask);
    protected abstract void onCompletion();

    /** Task Submission / Creation / Objects **/

    public <T> FutureTask<T> submit(Callable<T> task)
    {
        return submit(newTaskFor(task));
    }

    public FutureTask<?> submit(Runnable task)
    {
        return submit(newTaskFor(task, null));
    }

    public <T> FutureTask<T> submit(Runnable task, T result)
    {
        return submit(newTaskFor(task, result));
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
    {
        throw new UnsupportedOperationException();
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
    {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        throw new UnsupportedOperationException();
    }

    protected <T> FutureTask<T> newTaskFor(Runnable runnable, T result)
    {
        if (runnable instanceof FutureTask)
            return (FutureTask<T>) runnable;
        return new FutureTask<>(runnable, result);
    }

    protected <T> FutureTask<T> newTaskFor(Callable<T> callable)
    {
        if (callable instanceof FutureTask)
            return (FutureTask<T>) callable;
        return new FutureTask<>(callable);
    }


    public class FutureTask<T> extends SimpleCondition implements Future<T>, Runnable, PriorityProvider
    {
        private boolean failure;
        private Object result = this;
        private final Callable<T> callable;
        private Runnable runnable = null;

        public FutureTask(Callable<T> callable)
        {
            this.callable = callable;
        }
        public FutureTask(Runnable runnable, T result)
        {
            this(Executors.callable(runnable, result));
            this.runnable = runnable;
        }

        public void run()
        {
            try
            {
                result = callable.call();
            }
            catch (Throwable t)
            {
                logger.warn("Uncaught exception on thread {}: {}", Thread.currentThread(), t);
                result = t;
                failure = true;
            }
            finally
            {
                signalAll();
                onCompletion();
            }
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        public boolean isCancelled()
        {
            return false;
        }

        public boolean isDone()
        {
            return isSignaled();
        }

        public T get() throws InterruptedException, ExecutionException
        {
            await();
            Object result = this.result;
            if (failure)
                throw new ExecutionException((Throwable) result);
            return (T) result;
        }

        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            await(timeout, unit);
            Object result = this.result;
            if (failure)
                throw new ExecutionException((Throwable) result);
            return (T) result;
        }

        @Override
        public PriorityTuple getPriority() {
            //System.out.println("Getting priority of value: " + ((PriorityProvider) runnable).getPriority());
            if(runnable != null && runnable instanceof PriorityProvider)
                return ((PriorityProvider) runnable).getPriority();
            else
                throw new UnsupportedOperationException();
        }

        @Override
        public int getBatchSize() {
            //System.out.println("Getting priority of value: " + ((PriorityProvider) runnable).getPriority());
            if(runnable != null && runnable instanceof PriorityProvider)
                return ((PriorityProvider) runnable).getBatchSize();
            else
                throw new UnsupportedOperationException();
        }
    }

    private <T> FutureTask<T> submit(FutureTask<T> task)
    {
        addTask(task);
        return task;
    }

    public void execute(Runnable command)
    {
        addTask(newTaskFor(command, null));
    }
}
