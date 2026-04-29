import argparse
import csv
import sys
from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import copy


BASE_DIR = Path(__file__).resolve().parent
INPUTS_DIR = BASE_DIR / "inputs"
OUTPUTS_DIR = BASE_DIR / "outputs"


@dataclass(frozen=True)
class Job:
    job_id: str
    release: int
    deadline: int
    processing_time: int

    @property
    def length(self) -> int:
        return self.deadline - self.release


@dataclass
class Machine:
    jobs: List[Job]

    def busy_time(self) -> int:
        if not self.jobs:
            return 0
        intervals = sorted((job.release, job.deadline) for job in self.jobs)
        total = 0
        current_start, current_end = intervals[0]
        for start, end in intervals[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                total += current_end - current_start
                current_start, current_end = start, end
        total += current_end - current_start
        return total

    def is_feasible_with(self, job: Job, capacity: int) -> bool:
        return is_feasible_machine(self.jobs + [job], capacity)

    def add_job(self, job: Job) -> None:
        self.jobs.append(job)


def is_feasible_machine(jobs: List[Job], capacity: int) -> bool:
    events = []
    for job in jobs:
        events.append((job.release, 1))
        events.append((job.deadline, -1))
    events.sort(key=lambda x: (x[0], x[1]))
    active = 0
    for _, change in events:
        active += change
        if active > capacity:
            return False
    return True


def total_busy_time(schedule: List[Machine]) -> int:
    return sum(machine.busy_time() for machine in schedule)


def first_fit_initial_schedule(jobs: List[Job], capacity: int) -> List[Machine]:
    machines: List[Machine] = []
    for job in sorted(jobs, key=lambda j: (j.release, j.deadline, j.job_id)):
        placed = False
        for machine in machines:
            if machine.is_feasible_with(job, capacity):
                machine.add_job(job)
                placed = True
                break
        if not placed:
            machines.append(Machine(jobs=[job]))
    return machines


def greedy_repack(jobs: List[Job], capacity: int) -> List[Machine]:
    machines: List[Machine] = []
    for job in sorted(jobs, key=lambda j: (-j.length, j.release, j.deadline, j.job_id)):
        best_machine = None
        best_increase = None
        for machine in machines:
            if machine.is_feasible_with(job, capacity):
                old_cost = machine.busy_time()
                new_cost = Machine(machine.jobs + [job]).busy_time()
                increase = new_cost - old_cost
                if best_increase is None or increase < best_increase:
                    best_increase = increase
                    best_machine = machine
        if best_machine is not None:
            best_machine.add_job(job)
        else:
            machines.append(Machine(jobs=[job]))
    return machines


def local_search_busy_time(
    jobs: List[Job],
    capacity: int,
    b: int = 2,
    max_iterations: int = 100,
    verbose: bool = True,
) -> List[Machine]:
    """
    Busy-time b-local search.

    Each iteration selects a subset of up to b machines, repacks their jobs
    greedily, and accepts the move only if total busy time decreases.
    """
    schedule = first_fit_initial_schedule(jobs, capacity)

    if verbose:
        print(f"Initial schedule: cost={total_busy_time(schedule)}, machines={len(schedule)}")

    for iteration in range(1, max_iterations + 1):
        improvement_found = False
        current_cost = total_busy_time(schedule)

        for subset_size in range(1, min(b, len(schedule)) + 1):
            for selected in combinations(range(len(schedule)), subset_size):
                selected_set = set(selected)
                old_machines = [schedule[i] for i in selected]
                remaining = [copy.deepcopy(schedule[i]) for i in range(len(schedule)) if i not in selected_set]

                jobs_to_repack = [job for m in old_machines for job in m.jobs]
                old_local_cost = sum(m.busy_time() for m in old_machines)

                new_machines = greedy_repack(jobs_to_repack, capacity)
                new_local_cost = sum(m.busy_time() for m in new_machines)

                if new_local_cost < old_local_cost:
                    new_schedule = remaining + new_machines
                    new_cost = total_busy_time(new_schedule)
                    if new_cost < current_cost:
                        schedule = new_schedule
                        improvement_found = True
                        if verbose:
                            print(f"Iteration {iteration}: {current_cost} -> {new_cost} "
                                  f"(machines: {len(schedule)})")
                        break

            if improvement_found:
                break

        if not improvement_found:
            if verbose:
                print(f"Converged after {iteration} iteration(s). No further improvement.")
            break

    return schedule


# ──────────────────────────────────────────────────────────────────
# CSV parsing
# ──────────────────────────────────────────────────────────────────

def _parse_int(value: str, field: str, ctx: str = "") -> int:
    raw = value.strip()
    try:
        parsed = int(raw)
    except ValueError:
        suffix = f" ({ctx})" if ctx else ""
        raise ValueError(f"{field}{suffix} must be an integer, got {value!r}.")
    if str(parsed) != raw:
        suffix = f" ({ctx})" if ctx else ""
        raise ValueError(f"{field}{suffix} must be an integer, got {value!r}.")
    return parsed


def read_input_csv(path: Path) -> Tuple[int, List[Job]]:
    with path.open(mode="r", newline="", encoding="utf-8-sig") as f:
        rows = [row for row in csv.reader(f, skipinitialspace=True) if row]

    if len(rows) < 4:
        raise ValueError("Input CSV must have at least 4 rows (title, Capacity label, value, header).")
    if rows[1][0].strip() != "Capacity":
        raise ValueError("Row 2 must be exactly: Capacity")

    capacity = _parse_int(rows[2][0], "Capacity")
    if capacity <= 0:
        raise ValueError("Capacity must be a positive integer.")

    header = [c.strip() for c in rows[3]]
    if header != ["Job", "Release", "Deadline", "processingTime"]:
        raise ValueError("Job header must be: Job,Release,Deadline,processingTime")

    jobs: List[Job] = []
    seen: set = set()

    for i, row in enumerate(rows[4:], start=5):
        if len(row) < 4:
            continue
        jid = row[0].strip()
        r = _parse_int(row[1], "Release", f"row {i}")
        d = _parse_int(row[2], "Deadline", f"row {i}")
        p = _parse_int(row[3], "processingTime", f"row {i}")

        if not jid:
            raise ValueError(f"Row {i}: empty job id.")
        if jid in seen:
            raise ValueError(f"Row {i}: duplicate job id {jid!r}.")
        if d <= r:
            raise ValueError(f"Row {i}, job {jid}: Deadline must exceed Release.")
        if p <= 0 or p > d - r:
            raise ValueError(f"Row {i}, job {jid}: processingTime {p} is out of range [1, {d-r}].")

        seen.add(jid)
        jobs.append(Job(jid, r, d, p))

    if not jobs:
        raise ValueError("No jobs found in CSV.")

    return capacity, jobs


# ──────────────────────────────────────────────────────────────────
# CSV output
# ──────────────────────────────────────────────────────────────────

def preemptive_edf_per_machine(machine: Machine) -> List[Tuple[int, int, List[str]]]:
    """
    Run a preemptive EDF schedule on a single machine.

    Because the local search guarantees at most g windows overlap at any time,
    at each unit step we simply run every live job that still has remaining work.
    Jobs are completed in EDF order when there is a tie in eligibility.

    Returns a list of (start, end, [job_ids]) unit-length pieces.
    """
    if not machine.jobs:
        return []

    t_min = min(j.release for j in machine.jobs)
    t_max = max(j.deadline for j in machine.jobs)

    remaining = {j.job_id: j.processing_time for j in machine.jobs}
    job_by_id = {j.job_id: j for j in machine.jobs}

    pieces: List[Tuple[int, int, List[str]]] = []

    for t in range(t_min, t_max):
        eligible = [
            j for j in machine.jobs
            if j.release <= t < j.deadline and remaining[j.job_id] > 0
        ]
        # EDF: earliest deadline first; break ties by job_id
        eligible.sort(key=lambda j: (j.deadline, j.job_id))

        if eligible:
            selected_ids = [j.job_id for j in eligible]
            for jid in selected_ids:
                remaining[jid] -= 1
            pieces.append((t, t + 1, selected_ids))

    return pieces


def build_schedule_rows(schedule: List[Machine]) -> List[dict]:
    """
    Build bounded-schedule rows in the I{N:03d}_M{M:03d} format.

    Steps:
      1. Run preemptive EDF on each machine to get exact-processingTime pieces.
      2. Collect all active unit time slots across all machines, sorted globally.
      3. Assign each unique active time slot a global ordinal N (I{N:03d}).
      4. Each piece becomes one row: machine name = I{N:03d}_M{machine_num:03d}.
    """
    # Step 1: preemptive schedule per machine
    all_pieces: List[Tuple[int, int, int, List[str]]] = []  # (start, end, machine_num, jobs)
    for m_idx, machine in enumerate(schedule, start=1):
        for start, end, jobs in preemptive_edf_per_machine(machine):
            all_pieces.append((start, end, m_idx, jobs))

    # Step 2: global time-slot ordinals (only active slots count)
    active_starts = sorted(set(start for start, _, _, _ in all_pieces))
    time_ordinal = {t: i for i, t in enumerate(active_starts, start=1)}

    # Step 3: build rows sorted by (start, machine_num)
    rows = []
    for start, end, machine_num, jobs in sorted(all_pieces, key=lambda x: (x[0], x[2])):
        n = time_ordinal[start]
        rows.append({
            "Machine": f"I{n:03d}_M{machine_num:03d}",
            "Start": start,
            "End": end,
            "Length": end - start,
            "Jobs on Machine": ", ".join(jobs),
        })

    return rows


def write_bounded_schedule_csv(path: Path, rows: List[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Machine", "Start", "End", "Length", "Jobs on Machine"])
        for row in rows:
            writer.writerow([row["Machine"], row["Start"], row["End"],
                             row["Length"], row["Jobs on Machine"]])


def write_summary_csv(path: Path, capacity: int, bounded_busy_time: int, num_machines: int) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Machine capacity g", capacity])
        writer.writerow(["Bounded busy time", bounded_busy_time])
        writer.writerow(["Number of machines", num_machines])


def print_schedule(schedule: List[Machine]) -> None:
    print("\nFinal Schedule")
    print("==============")

    total_busy = 0
    for i, machine in enumerate(schedule, start=1):
        pieces = preemptive_edf_per_machine(machine)
        machine_busy = sum(end - start for start, end, _ in pieces)
        total_busy += machine_busy
        print(f"\n  Machine {i}  (busy time: {machine_busy})")
        for job in sorted(machine.jobs, key=lambda j: (j.release, j.deadline, j.job_id)):
            print(f"    Job {job.job_id}: [{job.release}, {job.deadline})  p={job.processing_time}")

    print(f"\nTotal busy time : {total_busy}")
    print(f"Number of machines: {len(schedule)}")


# ──────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────

def _resolve_input(filename: str) -> Path:
    p = Path(filename)
    if p.is_absolute() and p.is_file():
        return p
    for candidate in [INPUTS_DIR / p, BASE_DIR / p, Path.cwd() / p, p]:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"Input file not found: {filename}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Local search for the busy-time scheduling problem."
    )
    parser.add_argument("input_csv", help="Input CSV (e.g. inputs/input1.csv)")
    parser.add_argument("--b", type=int, default=3,
                        help="Neighborhood size: repack up to b machines at once (default 2)")
    parser.add_argument("--max-iterations", type=int, default=100,
                        help="Maximum local-search iterations (default 100)")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (default: outputs/<stem>_local_search)")
    args = parser.parse_args()

    try:
        input_path = _resolve_input(args.input_csv)
        capacity, jobs = read_input_csv(input_path)
    except Exception as exc:
        print(f"Error reading input: {exc}", file=sys.stderr)
        return 1

    stem = input_path.stem
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUTS_DIR / f"{stem}_local_search"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Input   : {input_path}")
    print(f"Jobs    : {len(jobs)}")
    print(f"Capacity: g={capacity}")
    print(f"Params  : b={args.b}, max_iterations={args.max_iterations}")
    print()

    schedule = local_search_busy_time(
        jobs=jobs,
        capacity=capacity,
        b=args.b,
        max_iterations=args.max_iterations,
        verbose=True,
    )

    print_schedule(schedule)

    schedule_rows = build_schedule_rows(schedule)
    bounded_busy = sum(row["Length"] for row in schedule_rows)

    schedule_path = output_dir / f"{stem}_local_search_bounded_schedule.csv"
    summary_path = output_dir / f"{stem}_local_search_summary.csv"

    write_bounded_schedule_csv(schedule_path, schedule_rows)
    write_summary_csv(summary_path, capacity, bounded_busy, len(schedule))

    print(f"\nOutputs written to: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
