import threading
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import numpy as np
from collections import deque
import time

def index_to_excel_column(n):
    """Convert number to letter"""
    letters = ""
    while n >= 0:
        letters = chr(n % 26 + 65) + letters
        n = n // 26 - 1
    return letters

def update_progress(v, max_v):
    progress['value'] = v
    progress['maximum'] = max_v
    progress.update_idletasks()

def select_file(entry_widget):
    """Open file dialog to select an Excel file."""
    file_path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
    if file_path:
        entry_widget.delete(0, tk.END)
        entry_widget.insert(0, file_path)

class OptimizedComparison:
    def __init__(self, tree, progress_callback, done_callback):
        self.tree = tree
        self.progress_callback = progress_callback
        self.done_callback = done_callback
        self.results_buffer = deque()
        self.buffer_size = 1000  # Batch processing size
        
    def compare_dataframes_vectorized(self, df1, df2, sheet_name):
        """Vectorized comparison"""
        differences = []
        
        common_cols = [col for col in df2.columns if col in df1.columns]
        if not common_cols:
            yield [], 0
            return
            
        df1_aligned = df1[common_cols].reset_index(drop=True)
        df2_aligned = df2[common_cols].reset_index(drop=True)
        
        # Match 
        min_rows = min(len(df1_aligned), len(df2_aligned))
        df1_aligned = df1_aligned.head(min_rows)
        df2_aligned = df2_aligned.head(min_rows)
        
        total_cells = len(df1_aligned) * len(common_cols)
        
        chunk_size = max(100, min_rows // 10)  # At least 100 rows or 10% of total rows
        processed_cells = 0
        
        for start_row in range(0, min_rows, chunk_size):
            end_row = min(start_row + chunk_size, min_rows)
            chunk_df1 = df1_aligned.iloc[start_row:end_row]
            chunk_df2 = df2_aligned.iloc[start_row:end_row]
            
            for col_idx, col in enumerate(common_cols):
                series1 = chunk_df1[col]
                series2 = chunk_df2[col]
                
                # handle NaN
                mask_diff = ~((series1.isna() & series2.isna()) | (series1 == series2))
                
                if mask_diff.any():
                    diff_indices = np.where(mask_diff)[0] + start_row
                    col_letter = index_to_excel_column(col_idx)
                    
                    for row_idx in diff_indices:
                        val1 = df1_aligned.iloc[row_idx][col]
                        val2 = df2_aligned.iloc[row_idx][col]
                        differences.append((sheet_name, row_idx+1, col_letter, col, val1, val2))
            
            processed_cells += len(chunk_df1) * len(common_cols)
            yield differences, total_cells
            differences = []  # Reset
        
        extra_rows = len(df1) - len(df2)
        if extra_rows != 0:
            desc = f"Extra rows in {'File1' if extra_rows > 0 else 'File2'}"
            yield [(sheet_name, '-', '-', desc, len(df1), len(df2))], total_cells

def compare_files():
    """Compare all sheets of two Excel files and display differences"""
    path1 = entry_file1.get()
    path2 = entry_file2.get()
    if not path1 or not path2:
        messagebox.showwarning("Warning", "Please select two files (A and B) before comparing.")
        return

    try:
        # Read by chunk for large files
        sheets1 = pd.read_excel(path1, sheet_name=None, engine='openpyxl')
        sheets2 = pd.read_excel(path2, sheet_name=None, engine='openpyxl')
    except Exception as e:
        messagebox.showerror("Error", f"Failed to load Excel files:\n{e}")
        return

    # Clear prev results
    for row in tree.get_children():
        tree.delete(row)
    integrity_label.set("")

    common_sheets = sorted(set(sheets1) & set(sheets2))
    if not common_sheets:
        messagebox.showinfo("Info", "No common sheets found.")
        return
        
    compare_btn.config(text="Comparing...", state="disabled")
    progress['value'] = 0
    
    def progress_callback(current, total):
        root.after(0, lambda: update_progress(current, total))
    
    def insert_results_batch(results_batch):
        for result in results_batch:
            tree.insert('', 'end', values=result, tags=('diff',))
    
    def worker():
        start_time = time.time()
        total_cells_processed = 0
        total_differences = 0
        all_total_cells = 0

        estimated_total = sum(
            min(len(sheets1[sheet]), len(sheets2[sheet])) * 
            len([col for col in sheets2[sheet].columns if col in sheets1[sheet].columns])
            for sheet in common_sheets
        )
        
        comparator = OptimizedComparison(tree, progress_callback, None)
        
        try:
            for sheet_idx, sheet_name in enumerate(common_sheets):
                df1 = sheets1[sheet_name]
                df2 = sheets2[sheet_name]
                
                common_cols = [col for col in df2.columns if col in df1.columns]
                sheet_cells = min(len(df1), len(df2)) * len(common_cols)
                
                # Batch processing
                batch_count = 0
                for batch_results, sheet_total_cells in comparator.compare_dataframes_vectorized(df1, df2, sheet_name):
                    all_total_cells = max(all_total_cells, sheet_total_cells)
                    batch_count += 1
                    
                    # Show on GUI (thread-safe)
                    if batch_results:
                        total_differences += len(batch_results)
                        root.after(0, lambda batch=batch_results: insert_results_batch(batch))
                    
                    # Update progress
                    estimated_progress = total_cells_processed + (batch_count * 1000)
                    progress_callback(min(estimated_progress, estimated_total), estimated_total)
                
                total_cells_processed += sheet_cells
                progress_callback(total_cells_processed, estimated_total)
                
        except Exception as e:
            root.after(0, lambda: messagebox.showerror("Error", f"Comparison failed: {e}"))
            return
        
        def on_done():
            elapsed = time.time() - start_time
            
            # Integrity
            if all_total_cells > 0:
                percent = 100 * (1 - total_differences / all_total_cells)
                integrity_text = f"Data Integrity: {percent:.2f}% ({all_total_cells-total_differences:,}/{all_total_cells:,})"
                integrity_text += f" | Time: {elapsed:.1f}s | Speed: {all_total_cells/elapsed:,.0f} cells/s"
            else:
                integrity_text = "No matching cells found."
            
            integrity_label.set(integrity_text)
            compare_btn.config(text="Compare", state="normal")
            progress.config(value=0)
            
        root.after(0, on_done)
        
    # Daemon
    threading.Thread(target=worker, daemon=True).start()

# --- GUI ---
root = tk.Tk()
root.title("Excel Diff Compare Tool - Optimized")
root.configure(bg="#f3e5f5")
root.geometry("1000x780")

style = ttk.Style(root)
style.theme_use('clam')
style.configure('TButton', background='#6a0dad', foreground='white', font=('Arial', 10, 'bold'), padding=6)
style.map('TButton', background=[('active', '#580a85')])
style.configure('Treeview', rowheight=24, font=('Consolas', 10), background='white', fieldbackground='white')
style.configure('Treeview.Heading', font=('Arial', 11, 'bold'), background='#d8bfd8')

# Files
label1 = ttk.Label(root, text="File 1:", background="#f3e5f5")
label1.place(x=20, y=20)
entry_file1 = ttk.Entry(root, width=75)
entry_file1.place(x=100, y=20)
select_btn1 = ttk.Button(root, text="Browse", command=lambda: select_file(entry_file1))
select_btn1.place(x=820, y=16)

label2 = ttk.Label(root, text="File 2:", background="#f3e5f5")
label2.place(x=20, y=60)
entry_file2 = ttk.Entry(root, width=75)
entry_file2.place(x=100, y=60)
select_btn2 = ttk.Button(root, text="Browse", command=lambda: select_file(entry_file2))
select_btn2.place(x=820, y=56)

# Button
compare_btn = ttk.Button(root, text="Compare", command=compare_files)
compare_btn.place(x=440, y=100)

# Progress bar
progress = ttk.Progressbar(root, orient="horizontal", length=400, mode="determinate")
style.configure('Horizontal.TProgressbar', troughcolor='white', background='#6a0dad')
progress.place(x=300, y=140)

# Results
tree_frame = ttk.Frame(root)
tree_frame.place(x=20, y=170)
columns = ("Sheet", "Row", "Col", "Col Name", "Value A", "Value B")
tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=20)
for c in columns:
    tree.heading(c, text=c)
    tree.column(c, width=140, anchor='center')

tree.tag_configure('diff', background='#ffe6e6')

# Scrollbar
scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=tree.yview)
tree.configure(yscrollcommand=scrollbar.set)
tree.grid(row=0, column=0)
scrollbar.grid(row=0, column=1, sticky='ns')

# Integrity
integrity_label = tk.StringVar()
label_integrity = ttk.Label(root, textvariable=integrity_label, background="#f3e5f5", font=('Arial', 12, 'bold'))
label_integrity.place(x=20, y=740)

root.mainloop()

# By
#  ██████  ██░ ██  ▄▄▄      ▓█████▄  ▒█████   █     █░   ▓█████▄ ▓█████ ██▒   █▓
# ▒██    ▒ ▓██░ ██▒▒████▄    ▒██▀ ██▌▒██▒  ██▒▓█░ █ ░█░   ▒██▀ ██▌▓█   ▀▓██░   █▒
# ░ ▓██▄   ▒██▀▀██░▒██  ▀█▄  ░██   █▌▒██░  ██▒▒█░ █ ░█    ░██   █▌▒███   ▓██  █▒░
#   ▒   ██▒░▓█ ░██ ░██▄▄▄▄██ ░▓█▄   ▌▒██   ██░░█░ █ ░█    ░▓█▄   ▌▒▓█  ▄  ▒██ █░░
# ▒██████▒▒░▓█▒░██▓ ▓█   ▓██▒░▒████▓ ░ ████▓▒░░░██▒██▓    ░▒████▓ ░▒████▒  ▒▀█░  
# ▒ ▒▓▒ ▒ ░ ▒ ░░▒░▒ ▒▒   ▓▒█░ ▒▒▓  ▒ ░ ▒░▒░▒░ ░ ▓░▒ ▒      ▒▒▓  ▒ ░░ ▒░ ░  ░ ▐░  
# ░ ░▒  ░ ░ ▒ ░▒░ ░  ▒   ▒▒ ░ ░ ▒  ▒   ░ ▒ ▒░   ▒ ░ ░      ░ ▒  ▒  ░ ░  ░  ░ ░░  
# ░  ░  ░   ░  ░░ ░  ░   ▒    ░ ░  ░ ░ ░ ░ ▒    ░   ░      ░ ░  ░    ░       ░░  
#       ░   ░  ░  ░      ░  ░   ░        ░ ░      ░          ░       ░  ░     ░  