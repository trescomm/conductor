/*
 *  Copyright 2021 Netflix, Inc.
 *  <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_WAIT;

@Component(TASK_TYPE_WAIT)
public class Wait extends WorkflowSystemTask {

    public static final String RESUME_AFTER_WAIT_SECONDS_PARAM = "resumeAfterWaitSeconds";
    private static final Logger LOGGER = LoggerFactory.getLogger(Wait.class);

    public Wait() {
        super(TASK_TYPE_WAIT);
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor workflowExecutor) {
        if(task.getInputData().containsKey(RESUME_AFTER_WAIT_SECONDS_PARAM)) {
            long secondsToWait = Long.parseLong(String.valueOf(task.getInputData().get(RESUME_AFTER_WAIT_SECONDS_PARAM)));
            if(task.getCallbackAfterSeconds() != secondsToWait) {
                task.setCallbackAfterSeconds(secondsToWait);
                if (task.getStartTime() == 0) {
                    task.setStartTime(System.currentTimeMillis());
                }
                LOGGER.info("Scheduling wait call back after {} seconds, task start time {}", secondsToWait, task.getStartTime());
            }
        }
        task.setStatus(Status.IN_PROGRESS);
    }

    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor workflowExecutor) {
        if(task.getInputData().containsKey(RESUME_AFTER_WAIT_SECONDS_PARAM)) {
            long secondsToWait = Long.parseLong(String.valueOf(task.getInputData().get(RESUME_AFTER_WAIT_SECONDS_PARAM)));
            if(task.getCallbackAfterSeconds() == secondsToWait) {
                long resumeTime = task.getStartTime() + (secondsToWait * 1_000);
                LOGGER.info("Checking if wait time is completed - wait until {}, current time {}, diff = {}, task start time {}, wait seconds {}",
                        resumeTime, System.currentTimeMillis(), (resumeTime - System.currentTimeMillis()), task.getStartTime(), secondsToWait);
                if (resumeTime <= System.currentTimeMillis()) {
                    task.setStatus(Status.COMPLETED);
                    return true;
                } else {
                    LOGGER.info("Wait time not passed, call back after {} seconds, start time: {}", task.getCallbackAfterSeconds(), task.getStartTime());
                }
            } else {
                LOGGER.info("Workflow not started, invoking start");
                start(workflow, task, workflowExecutor);
                return true;
            }
        }
        return false;
    }

    @Override
    public void cancel(Workflow workflow, Task task, WorkflowExecutor workflowExecutor) {
        task.setStatus(Status.CANCELED);
    }
}
