package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;

import java.util.function.Consumer;

public interface InMemoryAsyncWorkflowSystemTask {


    void start(Workflow workflow, Task task, WorkflowExecutor workflowExecutor, Consumer<Task> onComplete);

}
