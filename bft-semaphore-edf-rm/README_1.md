# Byzantine Fault Tolerance with Semaphores vs EDF and RM

Python implementation of Lamport's **Byzantine Generals** algorithm OM(m) using threads and semaphores,
and a comparison with the real-time scheduling algorithms **Earliest Deadline First (EDF)** and **Rate Monotonic (RM)**.

Author: Yasin (402124003) - Operating Systems course

## Part 1 - Byzantine Generals with semaphores

- Every general is a thread; general 0 is the commander.
- Messages go through mailboxes protected by a `mutex` semaphore and an `items` counting semaphore.
- Rounds are separated by a reusable two-phase barrier built only from semaphores.
- Traitors send contradictory values to different generals.

| Case | n | m | Traitors | IC1 | IC2 | Messages |
|---|---|---|---|---|---|---|
| B1 No traitor | 4 | 1 | none | OK | OK | 9 |
| B2 Traitor lieutenant | 4 | 1 | L3 | OK | OK | 9 |
| B3 Traitor commander | 4 | 1 | Commander | OK | OK | 9 |
| B4 Too few generals | 3 | 1 | L2 | OK | Violated | 4 |
| B5 Two traitor lieutenants | 7 | 2 | L5, L6 | OK | OK | 156 |
| B6 Traitor commander + lieutenant | 7 | 2 | Commander, L6 | OK | OK | 156 |
| B7 Three traitors, m = 2 | 7 | 2 | L4, L5, L6 | Violated | Violated | 156 |

Agreement is correct whenever n >= 3m + 1, as the theory predicts.

## Part 2 - BFT round as a real-time task (RM vs EDF)

The agreement round is a periodic task that shares a message bus (binary semaphore) with other tasks.
Four policies are compared: RM, RM + Priority Inheritance, EDF, EDF + Priority Inheritance.

| Scenario | U | RM | RM+PIP | EDF | EDF+PIP |
|---|---|---|---|---|---|
| R1 BFT n=4, light load | 0.600 | 0 | 0 | 0 | 0 |
| R2 Priority inversion | 0.833 | 1 | 0 | 1 | 0 |
| R3 BFT n=7, high load | 0.950 | 1 | 1 | 0 | 0 |
| R4 BFT n=7 + shared bus | 1.000 | 1 | 1 | 0 | 0 |

(numbers = deadline misses)

- Plain semaphores cause priority inversion under both RM and EDF; priority inheritance fixes it.
- At high load EDF meets all deadlines while RM fails where Response Time Analysis predicts.

## Files

- `bft_semaphore_rm_edf.py` - full program (Part 1 and Part 2)
- `bft_output.txt` - program output
- `BFT_Semaphore_vs_EDF_RM.docx` - full report (43 pages)

## Run

```
python bft_semaphore_rm_edf.py
```

Requires Python 3 only (no extra packages).
