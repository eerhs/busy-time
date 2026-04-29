# Busy-Time Scheduling

This repository contains a compact Python implementation of a busy-time
scheduling procedure based on the project file `theorem7.py`.

The program reads a set of jobs from a CSV file, computes an unbounded-capacity
preemptive schedule, converts that schedule into a bounded-capacity machine
schedule with capacity `g`, prints the results in the terminal, and writes
several CSV reports under `outputs/`.

## Project Structure

```text
busy-time/
+-- theorem7.py
+-- README.md
+-- inputs/
|   +-- jobs_c2.csv
+-- outputs/
    +-- jobs_c2/
        +-- jobs_c2_input_jobs.csv
        +-- jobs_c2_unbounded_active_intervals.csv
        +-- jobs_c2_unbounded_schedule.csv
        +-- jobs_c2_bounded_schedule.csv
        +-- jobs_c2_summary.csv
```

### Main Files

- `theorem7.py`: main implementation and command-line entry point.
- `inputs/`: default location for input CSV files.
- `outputs/`: generated result files. Each input file gets its own output
  folder named after the input filename stem.
- `README.md`: project documentation.

## Requirements

The project uses only the Python standard library:

- `csv`
- `sys`
- `dataclasses`
- `math`
- `pathlib`
- `typing`

No external dependencies are required.

Python 3.10 or newer is recommended because the code uses the `str | Path`
type annotation syntax.

## How to Run

From the repository root:

```bash
python theorem7.py jobs_c2.csv
```

By default, relative input filenames are resolved against the `inputs/`
directory first. The command above reads:

```text
inputs/jobs_c2.csv
```

You can also pass an absolute path or a CSV file located in the repository
root.

If no argument is provided, the script tries to read:

```text
inputs/jobs.csv
```

## Input CSV Format

Input files must follow this structure:

```csv
Busy Time
Capacity
2
Job,Release,Deadline,processingTime
A,3,6,1
B,3,9,3
```

### Fields

- `Capacity`: the maximum number of jobs that can run simultaneously on one
  machine. In the code this value is called `g`.
- `Job`: unique job identifier.
- `Release`: earliest time at which the job may start.
- `Deadline`: latest time by which the job must finish.
- `processingTime`: amount of processing time required by the job.

### Validation Rules

The parser checks that:

- The second row is exactly `Capacity`.
- `g` is a positive integer.
- The job header is exactly:

```csv
Job,Release,Deadline,processingTime
```

- Every job has a non-empty identifier.
- Each deadline is greater than its release time.
- Each processing time is positive.
- Each job is individually feasible:

```text
processingTime <= Deadline - Release
```

## Output Files

For an input file named `jobs_c2.csv`, the script writes results to:

```text
outputs/jobs_c2/
```

The generated files are:

- `jobs_c2_input_jobs.csv`: normalized copy of the jobs read from the input.
- `jobs_c2_unbounded_active_intervals.csv`: active intervals in the
  unbounded-capacity schedule.
- `jobs_c2_unbounded_schedule.csv`: preemptive schedule assigned to each job
  over the active intervals.
- `jobs_c2_bounded_schedule.csv`: bounded-capacity machine schedule.
- `jobs_c2_summary.csv`: global metrics, including unbounded busy time,
  bounded busy time, and their ratio.

## Code Overview

All implementation lives in `theorem7.py`. The file is organized into clear
sections.

### Constants and Paths

```python
EPS = 1e-9

BASE_DIR = Path(__file__).resolve().parent
INPUTS_DIR = BASE_DIR / "inputs"
LEGACY_INPUT_DIR = BASE_DIR / "input"
OUTPUTS_DIR = BASE_DIR / "outputs"
```

- `EPS` is used for floating-point comparisons.
- `BASE_DIR` points to the repository directory.
- `INPUTS_DIR` is the default input folder.
- `LEGACY_INPUT_DIR` supports an older `input/` folder name.
- `OUTPUTS_DIR` is where generated CSV files are written.

### Data Classes

The script defines three data classes:

```python
@dataclass(frozen=True)
class Job:
    id: str
    r: float
    d: float
    p: float
```

`Job` stores the input data for one job:

- `id`: job name.
- `r`: release time.
- `d`: deadline.
- `p`: processing time.

```python
@dataclass
class ScheduledPiece:
    job_id: str
    start: float
    end: float
```

`ScheduledPiece` represents one time segment assigned to a job in the
unbounded schedule.

```python
@dataclass
class MachinePiece:
    machine_id: str
    start: float
    end: float
    jobs: List[str]
```

`MachinePiece` represents one interval on one machine in the bounded schedule.
The `jobs` list contains the jobs running on that machine during the interval.

### Input Resolution and Parsing

`resolve_input_csv_path(filename)` searches for relative input files in this
order:

1. `inputs/<filename>`
2. `<repo-root>/<filename>`
3. current working directory
4. `input/<filename>`

`read_input_from_csv(filename)` reads the capacity and jobs, validates the
format, and returns:

```python
Tuple[int, List[Job]]
```

The integer is the machine capacity `g`; the list contains all parsed jobs.

### Interval Helpers

Several helper functions work with half-open intervals `[start, end)`:

- `merge_intervals(intervals)`: sorts and merges overlapping or touching
  intervals.
- `intersection_length(intervals, r, d)`: computes how much of a job window
  `[r, d)` is already covered by active intervals.
- `clip_intervals(intervals, r, d)`: restricts intervals to a job window.
- `add_latest_inactive_time(active, r, d, need)`: adds the latest possible
  inactive time inside `[r, d)` until `need` units of time have been added.

The last helper is important: it extends the active set as late as possible
inside each job window. If there is not enough available time, it raises an
infeasibility error.

### Step 1: Unbounded-Capacity Schedule

The first scheduling stage ignores the machine capacity limit.

`compute_unbounded_active_intervals(jobs)` processes jobs by increasing
deadline. For each job, it checks how much active time already exists inside
the job window. If the existing active time is not enough, it adds the missing
amount as late as possible.

The result is a compact set of active intervals called `S_infinity` in the
terminal output.

`assign_jobs_to_unbounded_schedule(jobs, active_intervals)` then assigns each
job to those active intervals. Jobs may be preempted, so a single job can have
multiple `ScheduledPiece` entries.

### Step 2: Bounded-Capacity Schedule

The second stage converts the unbounded schedule into a bounded-capacity
machine schedule.

`build_interesting_intervals(unbounded_schedule)` collects every start and end
point from the unbounded schedule, sorts them, and creates elementary intervals
between consecutive points.

`jobs_running_on_interval(unbounded_schedule, start, end)` finds all jobs that
cover one elementary interval.

`theorem_7_bounded_preemptive_schedule(jobs, g)` combines the complete process:

1. Compute unbounded active intervals.
2. Assign jobs to the unbounded preemptive schedule.
3. Split the timeline into interesting intervals.
4. For each interval, group running jobs into chunks of size `g`.
5. Assign each chunk to a machine piece.

The number of machines used in each interval is:

```python
ceil(number_of_running_jobs / g)
```

### Busy-Time Metrics

The script reports two main metrics:

- `total_unbounded_busy_time(active_intervals)`: sum of active interval
  lengths in the unbounded schedule.
- `total_bounded_busy_time(bounded_schedule)`: sum of all machine interval
  lengths in the bounded schedule.

The summary CSV also reports:

```text
Bounded / Unbounded ratio
```

### CSV Export Functions

The following functions write individual reports:

- `save_input_jobs_csv`
- `save_unbounded_active_intervals_csv`
- `save_unbounded_schedule_csv`
- `save_bounded_schedule_csv`
- `save_summary_csv`

`save_all_results_to_separate_csv_files(...)` creates the output directory and
writes all reports at once.

### Terminal Output

`print_results(...)` displays:

- Machine capacity.
- Jobs read from the CSV.
- Unbounded active intervals.
- Unbounded preemptive job schedule.
- Bounded machine schedule.
- Busy-time totals.

## Example Result

Running:

```bash
python theorem7.py jobs_c2.csv
```

with the included input produces a summary like:

```csv
Metric,Value
Machine capacity g,2
Unbounded busy time,24.0
Bounded busy time,157.0
Bounded / Unbounded ratio,6.541666666666667
```

## Notes

- Time values are handled as floats.
- Intervals are treated as half-open intervals: `[start, end)`.
- The implementation supports preemption, so a job can be split across
  multiple intervals.
- Output files are overwritten when the same input filename is run again.
