# Byzantine Fault Tolerance with Semaphores and Monitors vs EDF and RM

Lamport's **Byzantine Generals** algorithm OM(m) implemented twice in Python - with **semaphores**
and with **monitors** - and compared with the real-time scheduling algorithms
**Earliest Deadline First (EDF)** and **Rate Monotonic (RM)**.

Author: Yasin (402124003) - Operating Systems course

## Part 1 - Semaphore version vs monitor version

| | Semaphore version | Monitor version |
|---|---|---|
| Mailbox | `mutex` + `items` semaphores | lock + condition `not_empty` |
| Barrier | two-phase barrier (mutex + 2 turnstiles) | generation counter + `notify_all` |
| Primitives for n generals | 2n + 3 semaphores | n + 1 monitors |

| Case | n | m | Traitors | IC1 | IC2 | Semaphore ops (P+V) | Monitor ops (entries+waits+notifies) |
|---|---|---|---|---|---|---|---|
| B1 No traitor | 4 | 1 | none | OK | OK | 182 | 59 |
| B2 Traitor lieutenant | 4 | 1 | L3 | OK | OK | 182 | 59 |
| B3 Traitor commander | 4 | 1 | Commander | OK | OK | 182 | 59 |
| B4 Too few generals | 3 | 1 | L2 | OK | Violated | 120 | 36 |
| B5 Two traitor lieutenants | 7 | 2 | L5, L6 | OK | OK | 1272 | 552 |
| B6 Traitor commander + lieutenant | 7 | 2 | Commander, L6 | OK | OK | 1272 | 552 |
| B7 Three traitors, m = 2 | 7 | 2 | L4, L5, L6 | Violated | Violated | 1272 | 552 |

- Both versions give **identical decisions** in all cases; agreement is correct whenever n >= 3m + 1.
- The monitor version needs 57-70 % fewer synchronisation operations and runs about 1.7-2x faster.

## Part 2 - BFT round as a real-time task with a shared bus lock

The bus is protected by a lock (a binary semaphore or a monitor - for the scheduler they behave the same).
Six policies: RM, RM + Priority Inheritance, RM + Priority Ceiling, EDF, EDF + Inheritance, EDF + SRP.

| Scenario | U | RM | RM+PIP | RM+PCP | EDF | EDF+PIP | EDF+SRP |
|---|---|---|---|---|---|---|---|
| R1 BFT n=4, light load | 0.600 | 0 | 0 | 0 | 0 | 0 | 0 |
| R2 Priority inversion | 0.833 | 1 | 0 | 0 | 1 | 0 | 0 |
| R3 BFT n=7, high load | 0.950 | 1 | 1 | 1 | 0 | 0 | 0 |
| R4 BFT n=7 + shared bus | 1.000 | 1 | 1 | 1 | 0 | 0 | 0 |
| R5 Ceiling vs inheritance | 0.900 | 1 | 0 | 0 | 1 | 0 | 0 |

(numbers = deadline misses)

- A plain lock causes priority inversion under both RM and EDF; inheritance and ceiling protocols fix it.
- At high load EDF meets every deadline while RM fails where Response Time Analysis predicts.

## Files

- `bft_sem_monitor_rm_edf.py` - full program (both implementations + real-time experiment)
- `bft_sem_monitor_output.txt` - program output
- `BFT_Semaphore_Monitor_vs_EDF_RM.docx` - full report (47 pages)

## Run

```
python bft_sem_monitor_rm_edf.py
```

Requires Python 3 only (no extra packages).
