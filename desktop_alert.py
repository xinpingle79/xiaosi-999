import argparse
import tkinter as tk


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--title", required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("--critical", action="store_true")
    parser.add_argument("--info", action="store_true")
    return parser.parse_args()

def show_tk_alert(title, message, critical):
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    root.update_idletasks()

    dialog = tk.Toplevel(root)
    dialog.title(title)
    dialog.attributes("-topmost", True)
    dialog.resizable(False, False)
    dialog.configure(padx=18, pady=16)

    width = 520
    height = 220
    screen_width = dialog.winfo_screenwidth()
    screen_height = dialog.winfo_screenheight()
    pos_x = int((screen_width - width) / 2)
    pos_y = int((screen_height - height) / 2)
    dialog.geometry(f"{width}x{height}+{pos_x}+{pos_y}")

    dialog.grid_columnconfigure(0, weight=1)
    dialog.grid_rowconfigure(1, weight=1)

    title_color = "#b00020" if critical else "#1f3b73"
    title_label = tk.Label(
        dialog,
        text=title,
        font=("Arial", 14, "bold"),
        fg=title_color,
        anchor="w",
        justify="left",
    )
    title_label.grid(row=0, column=0, sticky="ew", pady=(0, 12))

    message_label = tk.Label(
        dialog,
        text=message,
        font=("Arial", 11),
        justify="left",
        anchor="nw",
        wraplength=480,
    )
    message_label.grid(row=1, column=0, sticky="nsew")

    button = tk.Button(
        dialog,
        text="确定",
        width=12,
        command=dialog.destroy,
    )
    button.grid(row=2, column=0, pady=(16, 0))

    dialog.transient(root)
    dialog.focus_force()
    button.focus_set()
    dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)
    root.wait_window(dialog)
    root.destroy()
    return True


def main():
    args = parse_args()
    critical = bool(args.critical and not args.info)
    return 0 if show_tk_alert(args.title, args.message, critical) else 1


if __name__ == "__main__":
    raise SystemExit(main())
