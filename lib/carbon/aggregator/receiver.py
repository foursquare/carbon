from carbon.instrumentation import increment
from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.conf import settings
from carbon.rewrite import RewriteRuleManager
from carbon import events

def process(metric, datapoint):
  increment('datapointsReceived')

  for rule in RewriteRuleManager.preRules:
    metric = rule.apply(metric)

  aggregate_metrics = []

  for rule in RuleManager.rules:
    aggregate_metric = rule.get_aggregate_metric(metric)

    if aggregate_metric is None:
      continue
    else:
      aggregate_metrics.append(aggregate_metric)

    buffer = BufferManager.get_buffer(aggregate_metric)

    if not buffer.configured:
      buffer.configure_aggregation(rule.frequency, rule.aggregation_func)

    buffer.input(datapoint)

  if len(aggregate_metrics) == 0 or not settings['AGGREGATION_SUPPRESS_ORIGINAL']:
    for rule in RewriteRuleManager.postRules:
      metric = rule.apply(metric)

    if metric not in aggregate_metrics:
      events.metricGenerated(metric, datapoint)
