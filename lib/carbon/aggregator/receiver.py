from carbon.instrumentation import increment
from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.rewrite import RewriteRuleManager
from carbon import events


def process(metric, datapoint):
  increment('datapointsReceived')

  for rule in RewriteRuleManager.preRules:
    metric = rule.apply(metric)

  aggregate_metrics = []

  for (rule, aggregate_metric) in RuleManager.get_aggregate_metrics(metric):
    aggregate_metrics.append(aggregate_metric)
    buffer = BufferManager.get_buffer(aggregate_metric)

    if not buffer.configured:
      buffer.configure_aggregation(rule.frequency, rule.aggregation_func)

    buffer.input(datapoint)

  for rule in RewriteRuleManager.postRules:
    metric = rule.apply(metric)

  if metric not in aggregate_metrics:
    events.metricGenerated(metric, datapoint)
