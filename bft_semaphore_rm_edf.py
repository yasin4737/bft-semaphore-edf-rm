# Byzantine Fault Tolerance (Lamport's Byzantine Generals, OM(m)) implemented
# with threads and semaphores, and compared with RM and EDF scheduling.
#
# Part 1: OM(m) agreement. Every general is a thread. Messages travel through
#         mailboxes protected by semaphores; rounds are separated by a
#         semaphore-based barrier.
# Part 2: the BFT agreement round as a periodic real-time task that shares the
#         message bus (a binary semaphore) with other tasks, scheduled by RM and
#         EDF, with and without priority inheritance.
import threading
from collections import Counter
from math import gcd, ceil
from functools import reduce
from itertools import permutations

ATTACK, RETREAT = "ATTACK", "RETREAT"


# =====================================================================
# Part 1 - Byzantine Generals with semaphores
# =====================================================================
class Mailbox:
    """Producer/consumer mailbox: 'mutex' protects the list, 'items' counts messages."""
    def __init__(self, stats):
        self.msgs = []
        self.mutex = threading.Semaphore(1)
        self.items = threading.Semaphore(0)
        self.stats = stats

    def put(self, msg):
        self.mutex.acquire(); self.stats.p()      # P(mutex)
        self.msgs.append(msg)
        self.mutex.release(); self.stats.v()      # V(mutex)
        self.items.release(); self.stats.v()      # V(items)

    def get(self):
        self.items.acquire(); self.stats.p()      # P(items) - waits for a message
        self.mutex.acquire(); self.stats.p()
        msg = self.msgs.pop(0)
        self.mutex.release(); self.stats.v()
        return msg


class Barrier:
    """Reusable two-phase barrier built only from semaphores (Downey)."""
    def __init__(self, n, stats):
        self.n, self.count, self.stats = n, 0, stats
        self.mutex = threading.Semaphore(1)
        self.turn1 = threading.Semaphore(0)
        self.turn2 = threading.Semaphore(0)

    def wait(self):
        s = self.stats
        self.mutex.acquire(); s.p()
        self.count += 1
        if self.count == self.n:
            for _ in range(self.n):
                self.turn1.release(); s.v()
        self.mutex.release(); s.v()
        self.turn1.acquire(); s.p()
        self.mutex.acquire(); s.p()
        self.count -= 1
        if self.count == 0:
            for _ in range(self.n):
                self.turn2.release(); s.v()
        self.mutex.release(); s.v()
        self.turn2.acquire(); s.p()


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.P = self.V = self.messages = 0

    def p(self):
        with self.lock:
            self.P += 1

    def v(self):
        with self.lock:
            self.V += 1

    def msg(self):
        with self.lock:
            self.messages += 1


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
    net = {"box": [Mailbox(stats) for _ in range(n)], "barrier": Barrier(n, stats), "stats": stats}
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
                ic1=ic1, ic2=ic2, messages=stats.messages, P=stats.P, V=stats.V,
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
# Part 2 - BFT round as a real-time task: RM vs EDF with a semaphore
# =====================================================================
MSGS_PER_UNIT = 4                       # message cost: 4 messages per time unit


def bft_cost(n, m):
    return ceil(om_messages(n, m) / MSGS_PER_UNIT)


class RTTask:
    def __init__(self, name, C, T, phase, cs, index):
        self.name, self.C, self.T, self.phase, self.index = name, C, T, phase, index
        self.cs = cs                    # (start, length) of the bus critical section, or None


class RTJob:
    def __init__(self, task, release):
        self.task, self.release = task, release
        self.deadline = release + task.T
        self.done = 0
        self.first_run = self.finish = None
        self.blocked = 0

    def label(self):
        return f"{self.task.name}.{(self.release - self.task.phase) // self.task.T + 1}"

    def needs_sem(self):
        cs = self.task.cs
        return cs is not None and cs[0] <= self.done < cs[0] + cs[1]


RT_ALGOS = ["RM", "RM+PIP", "EDF", "EDF+PIP"]


def base_prio(job, algo):
    return (job.task.T, job.task.index) if algo.startswith("RM") else (job.deadline, job.task.index)


def simulate_rt(spec, algo, trace=False):
    tasks = [RTTask(*s, i) for i, s in enumerate(spec)]
    H = reduce(lambda a, b: a * b // gcd(a, b), [t.T for t in tasks]) + max(t.phase for t in tasks)
    ready, jobs, missed, timeline, steps = [], [], [], [], []
    holder = None                       # job that holds the bus semaphore
    inversion = preempt = 0
    current = None
    for time in range(H):
        for j in list(ready):           # drop late jobs (a dropped holder frees the bus)
            if j.deadline <= time:
                ready.remove(j); missed.append(j)
                if holder is j:
                    holder = None
                if current is j:
                    current = None
        for t in tasks:
            if time >= t.phase and (time - t.phase) % t.T == 0:
                j = RTJob(t, time); jobs.append(j); ready.append(j)
        blocked = [j for j in ready if j.needs_sem() and holder is not None and holder is not j]
        runnable = [j for j in ready if j not in blocked]
        eff = {id(j): base_prio(j, algo) for j in ready}
        if algo.endswith("PIP") and holder is not None and blocked:
            eff[id(holder)] = min([eff[id(holder)]] + [base_prio(b, algo) for b in blocked])
        chosen = min(runnable, key=lambda j: (eff[id(j)], j is not current)) if runnable else None
        for b in blocked:
            b.blocked += 1
        # unbounded priority inversion: a blocked job outranks the running job,
        # and the running job is not the semaphore holder
        if chosen and chosen is not holder and any(base_prio(b, algo) < base_prio(chosen, algo) for b in blocked):
            inversion += 1
        if current is not None and chosen is not current and current in ready:
            preempt += 1
        current = chosen
        if trace:
            steps.append((time, [j.label() for j in runnable], [j.label() for j in blocked],
                          holder.label() if holder else "-", chosen.label() if chosen else "idle"))
        if chosen is None:
            timeline.append(None); continue
        if chosen.needs_sem() and holder is None:
            holder = chosen             # P(bus)
        if chosen.first_run is None:
            chosen.first_run = time
        chosen.done += 1
        timeline.append(chosen.task.name + ("*" if holder is chosen else ""))
        cs = chosen.task.cs
        if holder is chosen and chosen.done == cs[0] + cs[1]:
            holder = None               # V(bus)
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
    print("PART 1 - Byzantine Generals OM(m) with threads and semaphores")
    print("=" * 78)
    for name, n, m, tr in BFT_CASES:
        r = run_byzantine(n, m, tr)
        dec = " ".join(f"L{k}:{'A' if v == ATTACK else 'R'}{'(T)' if k in tr else ''}" for k, v in r["decisions"].items())
        print(f"{name:<36} n={n} m={m} traitors={r['traitors']} n>=3m+1:{r['bound_ok']}")
        print(f"   decisions: {dec}")
        print(f"   IC1 {'OK' if r['ic1'] else 'VIOLATED'} | IC2 {'OK' if r['ic2'] else 'VIOLATED'}"
              f" | messages {r['messages']} | P ops {r['P']} | V ops {r['V']}")
    print()
    print("=" * 78)
    print("PART 2 - BFT agreement task scheduled by RM and EDF (bus = semaphore)")
    print("=" * 78)
    for name, spec in RT_SCENARIOS.items():
        print(f"\n{name}   U = {utilization(spec):.3f}")
        for t in spec:
            print(f"   {t[0]:<6} C={t[1]} T={t[2]} phase={t[3]} critical section={t[4]}")
        print(f"   {'Algorithm':<9}{'Jobs':<6}{'Miss':<6}{'AvgR':<7}{'MaxR':<6}{'Blocked':<9}{'Inversion':<11}{'Preempt':<8}")
        for a in RT_ALGOS:
            r = simulate_rt(spec, a)
            print(f"   {a:<9}{r['jobs']:<6}{r['miss']:<6}{r['avg']:<7.2f}{r['maxr']:<6}{r['blocked']:<9}"
                  f"{r['inversion']:<11}{r['preempt']:<8} missed={r['missed']}")
    print("\nScalability sweep, OM(1) with n generals (SENSE 1/5, CTRL 2/10, BFT C/35):")
    print(f"   {'n':<4}{'msgs':<7}{'C':<5}{'U':<7}" + "".join(f"{a:<9}" for a in RT_ALGOS))
    for r in sweep():
        print(f"   {r['n']:<4}{r['msgs']:<7}{r['C']:<5}{r['U']:<7.3f}" + "".join(f"{r['miss'][a]:<9}" for a in RT_ALGOS))
