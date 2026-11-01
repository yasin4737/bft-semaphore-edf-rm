# Byzantine Fault Tolerance with Monitors vs EDF and RM

Python implementation of Lamport's **Byzantine Generals** algorithm OM(m) using threads and **monitors**
(lock + condition variables), and a comparison with the real-time scheduling algorithms
**Earliest Deadline First (EDF)** and **Rate Monotonic (RM)**.

Author: Yasin (402124003) - Operating Systems course

## Part 1 - Byzantine Generals with monitors

- Every general is a thread; general 0 is the commander.
- Each general has a `MailboxMonitor` (condition `not_empty`).
- Rounds are separated by a `BarrierMonitor` (generation counter + `notify_all`).
- Python `threading.Condition` has Mesa semantics, so every wait is inside a `while` loop.

| Case | n | m | Traitors | IC1 | IC2 | Messages | Monitor entries | Semaphore P ops (previous version) |
|---|---|---|---|---|---|---|---|---|
| B1 No traitor | 4 | 1 | none | OK | OK | 9 | 34 | 91 |
| B2 Traitor lieutenant | 4 | 1 | L3 | OK | OK | 9 | 34 | 91 |
| B3 Traitor commander | 4 | 1 | Commander | OK | OK | 9 | 34 | 91 |
| B4 Too few generals | 3 | 1 | L2 | OK | Violated | 4 | 20 | 60 |
| B5 Two traitor lieutenants | 7 | 2 | L5, L6 | OK | OK | 156 | 354 | 636 |
| B6 Traitor commander + lieutenant | 7 | 2 | Commander, L6 | OK | OK | 156 | 354 | 636 |
| B7 Three traitors, m = 2 | 7 | 2 | L4, L5, L6 | Violated | Violated | 156 | 354 | 636 |

Agreement is correct whenever n >= 3m + 1. The monitor version gives the same decisions as the
semaphore version with 55-70 % fewer synchronisation operations.

## Part 2 - BFT round as a real-time task with a shared bus monitor

Six policies: RM, RM + Priority Inheritance, RM + Priority Ceiling, EDF, EDF + Inheritance, EDF + Stack Resource Policy.

| Scenario | U | RM | RM+PIP | RM+PCP | EDF | EDF+PIP | EDF+SRP |
|---|---|---|---|---|---|---|---|
| R1 BFT n=4, light load | 0.600 | 0 | 0 | 0 | 0 | 0 | 0 |
| R2 Priority inversion | 0.833 | 1 | 0 | 0 | 1 | 0 | 0 |
| R3 BFT n=7, high load | 0.950 | 1 | 1 | 1 | 0 | 0 | 0 |
| R4 BFT n=7 + shared bus | 1.000 | 1 | 1 | 1 | 0 | 0 | 0 |
| R5 Ceiling vs inheritance | 0.900 | 1 | 0 | 0 | 1 | 0 | 0 |

(numbers = deadline misses)

- A plain monitor lock causes priority inversion under both RM and EDF.
- Inheritance and ceiling protocols remove the misses; the ceiling protocol also removes all blocking in R5.
- At high load EDF meets every deadline while RM fails where Response Time Analysis predicts.

## Files

- `bft_monitor_rm_edf.py` - full program (Part 1 and Part 2)
- `bft_monitor_output.txt` - program output
- `BFT_Monitor_vs_EDF_RM.docx` - full report (46 pages)

## Run

```
python bft_monitor_rm_edf.py
```

Requires Python 3 only (no extra packages).
