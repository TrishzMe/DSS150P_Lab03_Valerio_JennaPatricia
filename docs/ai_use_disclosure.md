# AI Use Disclosure

Course: DSS150P, Laboratory Activity #3
Student: Valerio, Jenna Patricia (2024102708)

In line with section 16 of the laboratory handout, I disclose that I used an AI assistant (Claude, by
Anthropic, through Claude Code) while completing this laboratory. The assistance is also visible in
the commit history (`Co-Authored-By` lines).

## How AI was used

- Profiling the provided sources to locate the defects the dataset guide leaves undocumented
  (duplicates, invalid price/quantity/status, orphan references, missing emails, dirty city values)
  before any rule was written.
- Drafting and debugging the pipeline modules (extract, staging/curated transforms, UPSERT load,
  validation, benchmark, partitioning), the unit tests, the Airflow DAG, and the Docker/Compose
  changes.
- Running the pipeline, benchmark, and Airflow scenarios on my machine and capturing their terminal
  output and UI screenshots as evidence. Evidence files are recorded command output. Two were
  corrected after capture (one mislabelled command line and one re-captured command output), and no
  result was changed.
- Drafting the documentation (goal write-ups, data dictionary, benchmark interpretation, reflection)
  from that evidence. Numbers quoted in the documents were re-checked against the output files.

## My own contribution and responsibility

- I supplied the Laboratory 3 handout and the starter repository, and set the requirements for how
  the work was delivered: follow the handout closely, commit in several meaningful steps per goal,
  and keep the code clean and modular.
- I provided the machine the work ran on (Python, Docker Desktop, and the PostgreSQL and Airflow
  containers), so every benchmark measurement and every run in the evidence comes from my
  environment, as section 9.2 of the handout requires.
- I reviewed the results as the work progressed and required fixes where something did not meet the
  laboratory instructions. For example, I had a usable Airflow password removed from `.env.example`
  and the README so that no credential is visible in any committed file.
- I checked how the Airflow screenshots were captured and whether the evidence formats meet what the
  handout asks for in section 12.
- I created the GitHub repository for the submission, and reviewed and revised the wording of this
  disclosure.
- I remain responsible for this submission, as the course policy requires.
