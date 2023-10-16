[//]: # ()
[//]: # (## Metrics container drop matters?)

[//]: # ()
[//]: # (Drop container null:)

[//]: # (* springstarter 1.2MB)

[//]: # (* ts auth mongo 4.4.15 86.2 kB)

[//]: # ()
[//]: # (not Drop container null:)

[//]: # (* springstarter 1.2 MB)

[//]: # (* ts auth mongo 4.4.15 86.2 kB)

[//]: # ()
[//]: # (no difference --> solution, drop container at end: data preserved and non matching values get dropped)

[//]: # ()
[//]: # (drop at end:)

[//]: # (* springstarter 805kB)

[//]: # (* ts-auth-mongo 4.4.15 86.2 kB)
___

n: 42
r: 6
-> 6 sekunden mit 3500 ergebnissesn


n: 47
r: 7
-> 76 sekunden mit 63360 ergebnissen

___

| 0-traceID | 0-spanID | 0-microservice | 1-traceID | 1-spanID | 1-microservice | 2-traceID | 2-spanID | 2-microservice | 3-traceID | 3-spanID | 3-microservice |
|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|
| 1         | 1        | ts-admin       | 1         | 2        | ts-seat        | 1         | 3        | ts-admin       | 1         | 4        | ts-security    |
|           |          |                |           |          |                |           |          |                |           |          |                |

zu

| 0-traceID | 0-spanID | ts-admin | 1-traceID | 1-spanID | ts-seat | 2-traceID | 2-spanID | ts-security | 3-traceID | 3-spanID | ts-another |
|-----------|----------|----------|-----------|----------|---------|-----------|----------|-------------|-----------|----------|------------|
| 1         | 1        | true     | 1         | 2        | true    | 1         | 4        | true        | -         | -        | -          |
| 1         | 3        | true     | 1         | 2        | true    | 1         | 4        | true        | -         | -        | -          |

___

bzw größeres Beispiel:


| 0-traceID | 0-spanID | 0-microservice | 1-traceID | 1-spanID | 1-microservice | 2-traceID | 2-spanID | 2-microservice | 3-traceID | 3-spanID | 3-microservice | 4-traceID | 4-spanID | 4-microservice | 5-traceID | 5-spanID | 5-microservice | 6-traceID | 6-spanID | 6-microservice |
|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|
| 1         | 1        | ts-admin       | 1         | 2        | ts-seat        | 1         | 3        | ts-admin       | 1         | 4        | ts-security    | 1         | 5        | ts-admin       | 1         | 6        | ts-seat        | 1         | 7        | ts-another     |
|           |          |                |           |          |                |           |          |                |           |          |                |           |          |                |           |          |                |           |          |                |

zu

| 0-traceID | 0-spanID | ts-admin | 1-traceID | 1-spanID | ts-seat | 2-traceID | 2-spanID | ts-security | 3-traceID | 3-spanID | ts-another |
|-----------|----------|----------|-----------|----------|---------|-----------|----------|-------------|-----------|----------|------------|
| 1         | 1        | true     | 1         | 2        | true    | 1         | 4        | true        | 1         | 7        | true       |
| 1         | 3        | true     | 1         | 2        | true    | 1         | 4        | true        | 1         | 7        | true       |
| 1         | 5        | true     | 1         | 2        | true    | 1         | 4        | true        | 1         | 7        | true       |
| 1         | 1        | true     | 1         | 6        | true    | 1         | 4        | true        | 1         | 7        | true       |
| 1         | 3        | true     | 1         | 6        | true    | 1         | 4        | true        | 1         | 7        | true       |
| 1         | 5        | true     | 1         | 6        | true    | 1         | 4        | true        | 1         | 7        | true       |
