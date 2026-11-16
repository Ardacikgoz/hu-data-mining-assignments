Kernel setup for `ass.ipynb`

This README creates a clean environment and explains how to pick the kernel in VS Code.

What I created for you:
- A fresh venv at `.venv_new` inside the project.
- A registered kernel named `Python (miningass2-newenv)` (user kernelspec).
- A tiny `starter.ipynb` notebook with a Hello World cell.

If VS Code doesn't show the new kernel yet, follow these steps:

1) Restart VS Code: Command Palette -> Developer: Reload Window.
2) Open `ass.ipynb` with the Notebook Editor (right-click -> Open With -> Notebook Editor).
3) Click the kernel picker at top-right (it may say "Select Kernel"). Look for one of:
   - Python (miningass2-newenv)
   - ~/Desktop/miningAss2/.venv_new/bin/python
4) If you see the kernel, select it and run the cell (select the cell and press Shift+Enter). You should see:
   Hello, World!

If you still don't see the kernel in the list:
- Ensure the VS Code extensions are installed and enabled: "Python" and "Jupyter" (Microsoft).
- Try selecting interpreter: Command Palette -> Python: Select Interpreter -> choose `~/Desktop/miningAss2/.venv_new/bin/python`.
- After selecting the interpreter, re-open `ass.ipynb` and try the kernel picker again.

Manual commands (if you want to reproduce in terminal):

```bash
cd ~/Desktop/miningAss2
python3 -m venv .venv_new
source .venv_new/bin/activate
python -m pip install --upgrade pip
pip install ipykernel jupyter
python -m ipykernel install --user --name miningass2-newenv --display-name "Python (miningass2-newenv)"
```

If anything errors during the steps above, copy the error message and paste it here and I'll help debug.
