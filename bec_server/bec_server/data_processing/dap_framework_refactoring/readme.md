# First draft for the DAP framework refactoring

## DAP blocks

## Next steps

### Short term

- Investigate how well existing lmfit service can be transferred to blocks?
- How much of the worker can we reuse to run what we currently do in main?
- When would we ever need this threaded? parallelized?
- Concept of workflows!
    - Validate input/output schema of blocks for workflows
    - GUI design -> visualize

### Long term

- Workers to exectute blocks, would that be parallelized?
- Handling multiple messages as input? -> Procedures
- WorkerManager, manage Workers, receive updates, spin up/down SLURM jobs?
