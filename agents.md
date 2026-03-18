
### always use UTF-8 WITHOUT BOM

### 🔄 Project Awareness & Context
- **Always read `PLANNING.md`** at the start of a new conversation to understand the project's architecture, goals, style, and constraints.
- **Check `TASK.md`** before starting a new task. If the task isn't listed, add it with a brief description and today's date.
- **Use consistent naming conventions, file structure, and architecture patterns** as described in `PLANNING.md`.
- **Update `requirementes.txt`** After adding new dependencies/libraries.

### 🧱 Code Structure & Modularity
- **Never create a file longer than 500 lines of code.** If a file approaches this limit, refactor by splitting it into modules or helper files.
- **Organize code into clearly separated modules**, grouped by feature or responsibility.
- **Use clear, consistent imports** (prefer relative imports within packages).

### 🧪 Testing & Reliability
- **Always create Pytest unit tests for new features** (functions, classes, routes, etc).
- **After updating any logic**, check whether existing unit tests need to be updated. If so, do it.
- **Tests should live in a `/tests` folder** mirroring the main app structure.
  - Include at least:
    - 1 test for expected use
    - 1 edge case
    - 1 failure case


### 📎 Style & Conventions
- **Use Python** as the primary language.
- **Follow PEP8**, use type hints, and format with `black`.
- **Use `pydantic` for data validation**.
- **Use `polars` for data manipulation.
- Use `playwrigth` for scrapping.
- Write **docstrings for every function** using the Google style:
  ```python
  def example():
      """
      Brief summary.

      Args:
          param1 (type): Description.

      Returns:
          type: Description.
      """
  ```

### 📚 Documentation & Explainability

Documentation should follow the following structure:
- `PLANNING.md` - High level direction, scope, tech, etc.
- `README.md` - Project Information, what the project is, how it works, structure, architecture, functionality, installation, configuration, example of output, etc.
- `TASKS.md` - Initial, actual and future tasks for the project.

- **Organize every document** to not contain information related to another document's topic.
- **Before updating** read the whole `README.md`, to understand the full context.
- **Before updating** read the whole `PLANNING.md`, to understand the full context.
- **Update `README.md`** when new features are added, dependencies change, or setup steps are modified.
- **Comment non-obvious code** and ensure everything is understandable to a entry-level developer.
- When writing complex logic, **add an inline `# Reason:` comment** explaining the why, not just the what.

### 🧠 AI Behavior Rules
- **Never assume missing context. Ask questions if uncertain.**
- **Never hallucinate libraries or functions** – only use known, verified Python packages. Look with context7 for its recent documentation.
- **Always confirm file paths and module names** exist before referencing them in code or tests.
- **Never delete or overwrite existing code** unless explicitly instructed to or if part of a task from `TASK.md`.
- **When iterating over a bug** always analyze the whole files that are related to the bug, and after analyze them do the needed changes.
- **When referencing a file** always read and analyze the whole file before proposing changes or making them.

### 🪢 GitHub & Branch Management
- **Always be aware of the current Git branch and project state.**
- **Decide when to create, switch, or merge branches** based on the nature of the task (e.g., use feature branches for new features, bugfix branches for fixes, and main for production-ready code).
- **Push changes to GitHub at logical milestones** (e.g., after completing a feature, fixing a bug, or updating documentation).
- **Communicate branch strategy and actions** (such as switching, merging, or pushing) clearly in the chat before executing them.
- **Ensure the repository remains clean and organized** by following best practices for branching, merging, and commit messages.


	