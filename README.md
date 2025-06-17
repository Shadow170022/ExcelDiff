# Excel Diff Compare Tool

A graphical tool to compare two Excel files (.xlsx) side by side, sheet by sheet and cell by cell. Ideal for spotting discrepancies in data across large workbooks.

## 📦 Requirements

- Python 3.8 or higher
- Works on Windows (can be adapted for macOS/Linux)

## 🚀 Features

- Cell-by-cell comparison of common sheets and columns.
- Visual display of differences (row, column, values).
- Progress bar and performance stats.
- Optimized for large Excel files.
- No console required — friendly graphical interface (Tkinter).

## 💻 Installation

1. **Clone the repository:**

2. **(Optional) Create and activate a virtual environment:**
```
python -m venv venv
venv\Scripts\activate    # On Windows
```

3. **Install the dependencies:**
```
pip install -r requirements.txt
```

4. **Run the application:**
```
python excel_diff_compare.py
```

## 🧪 How It Works

- Loads both Excel files.
- Compares common sheets and columns.
- Highlights mismatches in a table with sheet name, row, column, and values.
- Displays overall integrity percentage and performance statistics.

## 📁 Executable Version (No Python Needed)
You can download a standalone .exe version if you don’t want to install Python:
👉 [Download from Releases](https://github.com/Shadow170022/ExcelDiff/releases/tag/Turbo).
