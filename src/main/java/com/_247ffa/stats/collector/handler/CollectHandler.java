package com._247ffa.stats.collector.handler;

import com._247ffa.stats.collector.function.Collect;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

public class CollectHandler {

	@FunctionName("collect")
	public void execute(@TimerTrigger(name = "collectTrigger", schedule = "0 */1 * * * *") String timerInfo,
			ExecutionContext context) {
		new Collect().accept(timerInfo);
	}
}