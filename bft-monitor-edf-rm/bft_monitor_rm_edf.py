# Byzantine Fault Tolerance (Lamport's Byzantine Generals, OM(m)) implemented
# with threads and MONITORS, and compared with RM and EDF scheduling.
#
# Part 1: OM(m) agreement. Every general is a thread. Messages travel through
#         mailbox monitors; rounds are separated by a barrier monitor.
#         A monitor = one lock (mutual exclusion) + condition variables
#         (Python threading.Condition, Mesa semantics: signal-and-continue).
# Part 2: the BFT agreement round as a periodic real-time task that must enter a
#         shared BUS MONITOR, scheduled by RM and EDF with three monitor-lock
#         protocols: plain lock, priority inheritance, and priority ceiling.
import threading
from collections import Counter
from math import gcd, ceil
from functools import reduce
from itertools import permutations

ATTACK, RETREAT = "ATTACK", "RETREAT"


# =====================================================================
# Part 1 - Byzantine Generals with monitors
# =====================================================================
class Monitor:
    """Base monitor: one lock shared by all its condition variables."""
    def __init__(self, stats):
        self._lock = threading.Lock()
        self.stats = stats

    def condition(self):
        return threading.Condition(self._lock)

    def enter(self):
        self._lock.acquire(); self.stats.enter()

    def leave(self):
        self._lock.release()


class MailboxMonitor(Monitor):
    """Monitor with one condition 'not_empty' (producer/consumer)."""
    def __init__(self, stats):
        super().__init__(stats)
        self.msgs = []
        self.not_empty = self.condition()

    def put(self, msg):
        self.enter()
        try:
            self.msgs.append(msg)
            self.not_empty.notify(); self.stats.notify()
        finally:
            self.leave()

    def get(self):
        self.enter()
        try:
            while not self.msgs:                  # Mesa semantics -> re-check in a loop
                self.stats.wait(); self.not_empty.wait()
            return self.msgs.pop(0)
        finally:
            self.leave()


class BarrierMonitor(Monitor):
    """Reusable barrier: the last thread to arrive starts a new generation."""
    def __init__(self, n, stats):
        super().__init__(stats)
        self.n, self.count, self.generation = n, 0, 0
        self.all_arrived = self.condition()

    def wait(self):
        self.enter()
        try:
            gen = self.generation
            self.count += 1
            if self.count == self.n:
                self.count = 0
                self.generation += 1
                self.all_arrived.notify_all(); self.stats.notify()
            else:
                while gen == self.generation:
                    self.stats.wait(); self.all_arrived.wait()
        finally:
            self.leave()


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.entries = self.waits = self.notifies = self.messages = 0

    def _inc(self, name):
        with self.lock:
            setattr(self, name, getattr(self, name) + 1)

    def enter(self): self._inc("entries")
    def wait(self): self._inc("waits")
    def notify(self): self._inc("notifies")
    def msg(self): self._inc("messages")


def traitor_value(sender, receiver, path):
    """A traitor tells different generals different things (deterministic)."""
    return ATTACK if (receiver + len(path) + sender) % 2 == 0 else RETREAT


def majority(values):
    c = Counter(values)
    if c[ATTACK] > c[RETREAT]:
        return ATTACK
    return RETREAT                     # ties -> default RETREAT


class General(threading.Thread):
    """One general. id 0 is the commander, the others are lieutenants."""
    def __init__(self, gid, n, m, traitor, order, net):
        super().__init__()
        self.gid, self.n, self.m, self.traitor, self.order = gid, n, m, traitor, order
        self.net = net
        self.tree = {}                 # EIG tree: path (tuple) -> value
        self.decision = None

    def send(self, to, path, value):
        if self.traitor:
            value = traitor_value(self.gid, to, path)
        self.net["box"][to].put((path, value))
        self.net["stats"].msg()

    def run(self):
        n, m, net, me = self.n, self.m, self.net, self.gid
        # Round 1: the commander sends its order to every lieutenant
        if me == 0:
            for j in range(1, n):
                self.send(j, (0,), self.order)
        net["barrier"].wait()
        if me != 0:
            path, val = net["box"][me].get()
            self.tree[path] = val
        net["barrier"].wait()
        # Rounds 2 .. m+1: every lieutenant relays what it heard
        for r in range(2, m + 2):
            if me != 0:
                for path in [p for p in list(self.tree) if len(p) == r - 1 and me not in p]:
                    for j in range(1, n):
                        if j != me and j not in path:
                            self.send(j, path + (me,), self.tree[path])
            net["barrier"].wait()
            if me != 0:
                expected = count_incoming(n, r, me)
                for _ in range(expected):
                    path, val = net["box"][me].get()
                    self.tree[path] = val
            net["barrier"].wait()
        if me != 0:
            self.decision = self.resolve((0,))

    def resolve(self, path):
        if len(path) == self.m + 1:
            return self.tree.get(path, RETREAT)
        children = [self.resolve(path + (j,)) for j in range(1, self.n)
                    if j not in path and j != self.gid]
        return majority([self.tree.get(path, RETREAT)] + children)


def count_incoming(n, r, me):
    """Number of round-r messages lieutenant 'me' receives: one per path of length r
    that starts at 0, has distinct lieutenants, avoids 'me', and ends with a sender."""
    lieutenants = [x for x in range(1, n) if x != me]
    return sum(1 for _ in permutations(lieutenants, r - 1))


def om_messages(n, m):
    """Total messages of OM(m) with n generals."""
    total, prod = 0, 1
    for k in range(m + 1):
        prod *= (n - 1 - k) if k > 0 else (n - 1)
        total += prod
    return total


def run_byzantine(n, m, traitors, order=ATTACK):
    stats = Stats()
    net = {"box": [MailboxMonitor(stats) for _ in range(n)], "barrier": BarrierMonitor(n, stats), "stats": stats}
    gens = [General(i, n, m, i in traitors, order, net) for i in range(n)]
    for g in gens:
        g.start()
    for g in gens:
        g.join()
    loyal = [g for g in gens[1:] if not g.traitor]
    decisions = {g.gid: g.decision for g in gens[1:]}
    ic1 = len({g.decision for g in loyal}) <= 1
    ic2 = True if 0 in traitors else all(g.decision == order for g in loyal)
    return dict(n=n, m=m, traitors=sorted(traitors), order=order, decisions=decisions,
                ic1=ic1, ic2=ic2, messages=stats.messages, entries=stats.entries,
                waits=stats.waits, notifies=stats.notifies,
                bound_ok=n >= 3 * m + 1, trees={g.gid: dict(g.tree) for g in gens[1:]})


BFT_CASES = [
    ("B1 No traitor", 4, 1, set()),
    ("B2 Traitor lieutenant", 4, 1, {3}),
    ("B3 Traitor commander", 4, 1, {0}),
    ("B4 Too few generals", 3, 1, {2}),
    ("B5 Two traitor lieutenants", 7, 2, {5, 6}),
    ("B6 Traitor commander + lieutenant", 7, 2, {0, 6}),
    ("B7 Three traitors, m = 2", 7, 2, {4, 5, 6}),
]


# =====================================================================
# Part 2 - BFT round as a real-time task: RM vs EDF with a bus monitor
# =====================================================================
MSGS_PER_UNIT = 4                       # message cost: 4 messages per time unit


def bft_cost(n, m):
    return ceil(om_messages(n, m) / MSGS_PER_UNIT)


class RTTask:
    def __init__(self, name, C, T, phase, cs, index):
        self.name, self.C, self.T, self.phase, self.index = name, C, T, phase, index
        self.cs = cs                    # (start, length) inside the bus monitor, or None


class RTJob:
    def __init__(self, task, release):
        self.task, self.release = task, release
        self.deadline = release + task.T
        self.done = 0
        self.first_run = self.finish = None
        self.blocked = 0

    def label(self):
        return f"{self.task.name}.{(self.release - self.task.phase) // self.task.T + 1}"

    def needs_monitor(self):
        cs = self.task.cs
        return cs is not None and cs[0] <= self.done < cs[0] + cs[1]


# Monitor-lock protocols:
#   plain : the monitor lock is an ordinary mutex
#   PIP   : priority inheritance - the job inside the monitor inherits the
#           priority of the jobs waiting to enter it
#   PCP   : priority ceiling - the job inside the monitor runs at the monitor's
#           ceiling (highest priority of any task that uses it); for EDF this is
#           the Stack Resource Policy (preemption level = period)
RT_ALGOS = ["RM", "RM+PIP", "RM+PCP", "EDF", "EDF+PIP", "EDF+SRP"]


def base_prio(job, algo):
    return (job.task.T, job.task.index) if algo.startswith("RM") else (job.deadline, job.task.index)


def simulate_rt(spec, algo, trace=False):
    tasks = [RTTask(*s, i) for i, s in enumerate(spec)]
    H = reduce(lambda a, b: a * b // gcd(a, b), [t.T for t in tasks]) + max(t.phase for t in tasks)
    users = [t for t in tasks if t.cs]
    ceil_T = min(t.T for t in users) if users else None   # monitor ceiling (shortest period of a user)
    ready, jobs, missed, timeline, steps = [], [], [], [], []
    owner = None                        # job currently inside the bus monitor
    inversion = preempt = 0
    current = None
    for time in range(H):
        for j in list(ready):           # drop late jobs (a dropped owner leaves the monitor)
            if j.deadline <= time:
                ready.remove(j); missed.append(j)
                if owner is j:
                    owner = None
                if current is j:
                    current = None
        for t in tasks:
            if time >= t.phase and (time - t.phase) % t.T == 0:
                j = RTJob(t, time); jobs.append(j); ready.append(j)
        blocked = [j for j in ready if j.needs_monitor() and owner is not None and owner is not j]
        runnable = [j for j in ready if j not in blocked]
        key = {id(j): base_prio(j, algo) for j in ready}
        if owner is not None:
            if algo.endswith("PIP") and blocked:
                key[id(owner)] = min([key[id(owner)]] + [base_prio(b, algo) for b in blocked])
            elif algo == "RM+PCP":
                key[id(owner)] = min(key[id(owner)], (ceil_T, -1))
        if algo == "EDF+SRP" and owner is not None:
            # SRP: while the monitor is in use, only jobs with a preemption level
            # higher than the ceiling (period < ceiling) may preempt the owner
            runnable = [j for j in runnable if j is owner or j.task.T < ceil_T]
        chosen = min(runnable, key=lambda j: (key[id(j)], j is not current)) if runnable else None
        for b in blocked:
            b.blocked += 1
        if chosen and chosen is not owner and any(base_prio(b, algo) < base_prio(chosen, algo) for b in blocked):
            inversion += 1
        if current is not None and chosen is not current and current in ready:
            preempt += 1
        current = chosen
        if trace:
            steps.append((time, [j.label() for j in runnable], [j.label() for j in blocked],
                          owner.label() if owner else "-", chosen.label() if chosen else "idle"))
        if chosen is None:
            timeline.append(None); continue
        if chosen.needs_monitor() and owner is None:
            owner = chosen              # enter monitor
        if chosen.first_run is None:
            chosen.first_run = time
        chosen.done += 1
        timeline.append(chosen.task.name + ("*" if owner is chosen else ""))
        cs = chosen.task.cs
        if owner is chosen and chosen.done == cs[0] + cs[1]:
            owner = None                # leave monitor
        if chosen.done == chosen.task.C:
            chosen.finish = time + 1; ready.remove(chosen); current = None
    for j in ready:
        if j.deadline <= H and j not in missed:
            missed.append(j)
    counted = [j for j in jobs if j.deadline <= H]
    done = [j for j in counted if j.finish is not None]
    resp = [j.finish - j.release for j in done]
    per = {}
    for t in tasks:
        js = [j for j in counted if j.task is t]
        dn = [j for j in js if j.finish is not None]
        rs = [j.finish - j.release for j in dn]
        per[t.name] = dict(jobs=len(js), missed=len(js) - len(dn), max=max(rs) if rs else None,
                           avg=sum(rs) / len(rs) if rs else None, blocked=sum(j.blocked for j in js))
    return dict(H=H, timeline=timeline, missed=[(j.label(), j.deadline) for j in missed], steps=steps,
                jobs=len(counted), done=len(done), miss=len(missed),
                avg=sum(resp) / len(resp) if resp else 0, maxr=max(resp) if resp else 0,
                inversion=inversion, preempt=preempt, per=per,
                blocked=sum(j.blocked for j in counted),
                cpu=100 * sum(1 for x in timeline if x) / H)


# Real-time scenarios: (name, C, T, phase, (cs_start, cs_len) or None)
C4 = bft_cost(4, 1)      # 3 units
C7 = bft_cost(7, 1)      # 9 units
RT_SCENARIOS = {
    "R1 BFT n=4, light load": [("SENSE", 1, 5, 0, None), ("BFT", C4, 10, 0, (0, C4)), ("LOG", 2, 20, 0, (0, 2))],
    "R2 Priority inversion": [("BFT", C4, 8, 1, (0, C4)), ("CTRL", 4, 12, 1, None), ("LOG", 3, 24, 0, (0, 3))],
    "R3 BFT n=7, high load": [("SENSE", 2, 6, 0, None), ("CTRL", 2, 12, 0, None), ("BFT", C7, 20, 0, (0, 6))],
    "R4 BFT n=7 + shared bus": [("SENSE", 1, 5, 0, None), ("BFT", C7, 20, 0, (3, 6)), ("CTRL", 2, 8, 0, None), ("LOG", 2, 20, 0, (0, 2))],
    "R5 Ceiling vs inheritance": [("BFT", C4, 6, 4, (0, C4)), ("CTRL", 4, 15, 1, None), ("LOG", 4, 30, 0, (0, 4))],
}


def sweep():
    """OM(1) with a growing number of generals n; BFT period 35."""
    rows = []
    for n in range(4, 13):
        C = bft_cost(n, 1)
        spec = [("SENSE", 1, 5, 0, None), ("CTRL", 2, 10, 0, None), ("BFT", C, 35, 0, (0, min(C, 6)))]
        rows.append(dict(n=n, m=1, msgs=om_messages(n, 1), C=C, U=utilization(spec),
                         miss={a: simulate_rt(spec, a)["miss"] for a in RT_ALGOS}))
    return rows


def utilization(spec):
    return sum(s[1] / s[2] for s in spec)


if __name__ == "__main__":
    print("=" * 78)
    print("PART 1 - Byzantine Generals OM(m) with threads and monitors")
    print("=" * 78)
    for name, n, m, tr in BFT_CASES:
        r = run_byzantine(n, m, tr)
        dec = " ".join(f"L{k}:{'A' if v == ATTACK else 'R'}{'(T)' if k in tr else ''}" for k, v in r["decisions"].items())
        print(f"{name:<36} n={n} m={m} traitors={r['traitors']} n>=3m+1:{r['bound_ok']}")
        print(f"   decisions: {dec}")
        print(f"   IC1 {'OK' if r['ic1'] else 'VIOLATED'} | IC2 {'OK' if r['ic2'] else 'VIOLATED'}"
              f" | messages {r['messages']} | monitor entries {r['entries']} | waits {r['waits']} | notifies {r['notifies']}")
    print()
    print("=" * 78)
    print("PART 2 - BFT agreement task scheduled by RM and EDF (bus = monitor)")
    print("=" * 78)
    for name, spec in RT_SCENARIOS.items():
        print(f"\n{name}   U = {utilization(spec):.3f}")
        for t in spec:
            print(f"   {t[0]:<6} C={t[1]} T={t[2]} phase={t[3]} in monitor={t[4]}")
        print(f"   {'Algorithm':<9}{'Jobs':<6}{'Miss':<6}{'AvgR':<7}{'MaxR':<6}{'Blocked':<9}{'Inversion':<11}{'Preempt':<8}")
        for a in RT_ALGOS:
            r = simulate_rt(spec, a)
            print(f"   {a:<9}{r['jobs']:<6}{r['miss']:<6}{r['avg']:<7.2f}{r['maxr']:<6}{r['blocked']:<9}"
                  f"{r['inversion']:<11}{r['preempt']:<8} missed={r['missed']}")
    print("\nScalability sweep, OM(1) with n generals (SENSE 1/5, CTRL 2/10, BFT C/35):")
    print(f"   {'n':<4}{'msgs':<7}{'C':<5}{'U':<7}" + "".join(f"{a:<9}" for a in RT_ALGOS))
    for r in sweep():
        print(f"   {r['n']:<4}{r['msgs']:<7}{r['C']:<5}{r['U']:<7.3f}" + "".join(f"{r['miss'][a]:<9}" for a in RT_ALGOS))
