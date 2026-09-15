#!/usr/bin/env python3
"""Fail when any Get workload's median exceeds phuslu by more than 5%."""
import re
import statistics
import sys

text = open(sys.argv[1]).read()
if '\nPASS\n' not in text:
    sys.exit('Benchmark run did not complete successfully')
samples = {}
for line in text.splitlines():
    match = re.match(r'(BenchmarkGet\w+/\w+)/(phuslu|xlru)-\d+\s+\d+\s+([\d.]+) ns/op', line)
    if match:
        case, implementation, value = match.groups()
        samples.setdefault(case, {}).setdefault(implementation, []).append(float(value))
expected = {f'BenchmarkGet{key}/{workload}' for key in ('Int64', 'String')
            for workload in ('Hot', 'Sliding', 'Distributed', 'Parallel', 'Miss')}
if samples.keys() != expected:
    sys.exit(f'Incomplete benchmark matrix: missing {expected - samples.keys()}')
failed = False
for case, values in samples.items():
    if set(values) != {'phuslu', 'xlru'}:
        sys.exit(f'Missing comparison for {case}')
    if min(map(len, values.values())) < 3:
        sys.exit(f'Need at least three samples for {case}')
    reference = statistics.median(values['phuslu'])
    actual = statistics.median(values['xlru'])
    ratio = actual / reference
    passed = ratio <= 1.05
    print(f'{case}: {reference:.3f} -> {actual:.3f} ns/op, {(ratio-1)*100:+.2f}% [{"PASS" if passed else "FAIL"}]')
    failed |= not passed
sys.exit(failed)
