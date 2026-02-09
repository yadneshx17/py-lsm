#!/usr/bin/env python3
"""
py-lsm CLI - Interactive Key-Value Store
A simple LSM-tree based database CLI

Commands:
  SET <key> <value>  - Store a key-value pair
  GET <key>          - Retrieve a value by key
  DEL <key>          - Delete a key (tombstone)
  FLUSH              - Force flush memtable to SSTable
  STATS              - Show database statistics
  KEYS               - Show all keys in memtable
  HELP               - Show this help
  EXIT / QUIT        - Exit the CLI
"""

import sys
import readline  # enables arrow keys and command history
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from lsm_engine import LSMEngine


class CLI:
    """Interactive CLI for py-lsm database"""
    
    PROMPT = "\033[1;36mpy-lsm>\033[0m "  # Cyan colored prompt
    
    HELP_TEXT = """
\033[1;33m╔══════════════════════════════════════════════════════════════╗
║                    py-lsm Database CLI                        ║
╚══════════════════════════════════════════════════════════════╝\033[0m

\033[1;32mCommands:\033[0m
  \033[1mSET\033[0m <key> <value>  Store a key-value pair
  \033[1mGET\033[0m <key>          Retrieve a value by key
  \033[1mDEL\033[0m <key>          Delete a key (marks as tombstone)
  \033[1mFLUSH\033[0m              Force flush memtable to disk
  \033[1mSTATS\033[0m              Show database statistics
  \033[1mKEYS\033[0m               List all keys in memtable
  \033[1mCLEAR\033[0m              Clear the screen
  \033[1mHELP\033[0m               Show this help message
  \033[1mEXIT\033[0m / \033[1mQUIT\033[0m        Exit the database

\033[1;32mExamples:\033[0m
  SET name yadnesh
  SET user:1 {"name": "alice", "age": 25}
  GET name
  DEL name
"""

    def __init__(self, db_folder="lsm_db", capacity=100):
        self.engine = LSMEngine(db_folder=db_folder, capacity=capacity)
        self.running = True
    
    def print_banner(self):
        """Print welcome banner"""
        print("\033[1;35m")
        print("╔═══════════════════════════════════════════╗")
        print("║     py-lsm: LSM-Tree Key-Value Store      ║")
        print("║        Type 'HELP' for commands           ║")
        print("╚═══════════════════════════════════════════╝")
        print("\033[0m")
    
    def success(self, msg):
        """Print success message in green"""
        print(f"\033[1;32m{msg}\033[0m")
    
    def error(self, msg):
        """Print error message in red"""
        print(f"\033[1;31mError: {msg}\033[0m")
    
    def info(self, msg):
        """Print info message in yellow"""
        print(f"\033[1;33m{msg}\033[0m")
    
    def cmd_set(self, args):
        """SET <key> <value>"""
        if len(args) < 2:
            self.error("Usage: SET <key> <value>")
            return
        
        key = args[0]
        value = " ".join(args[1:])  # Allow spaces in value
        self.engine.set(key, value)
        self.success(f"OK")
    
    def cmd_get(self, args):
        """GET <key>"""
        if len(args) < 1:
            self.error("Usage: GET <key>")
            return
        
        key = args[0]
        value = self.engine.get(key)
        
        if value is not None:
            print(f"\033[1;37m{value}\033[0m")
        else:
            self.info(f"(nil)")
    
    def cmd_del(self, args):
        """DEL <key> - Delete by setting tombstone"""
        if len(args) < 1:
            self.error("Usage: DEL <key>")
            return
        
        key = args[0]
        # In LSM trees, delete is a tombstone (special marker)
        # For now, we'll just acknowledge - full tombstone support would need engine changes
        self.info(f"DEL not fully implemented yet (requires tombstone support)")
    
    def cmd_flush(self, args):
        """FLUSH - Force flush memtable to SSTable"""
        self.engine.flush()
        self.success("Memtable flushed to SSTable")
    
    def cmd_stats(self, args):
        """STATS - Show database statistics"""
        memtable_size = len(self.engine.memtable)
        sstable_count = len(self.engine.index_cache)
        
        print("\n\033[1;33m Database Statistics\033[0m")
        print("─" * 35)
        print(f"  Memtable entries:  {memtable_size}")
        print(f"  SSTable files:     {sstable_count}")
        print(f"  Capacity:          {self.engine.capacity}")
        print(f"  DB folder:         {self.engine.db_folder}")
        
        if sstable_count > 0:
            print("\n  \033[1mSSTable files:\033[0m")
            for sst_path in self.engine.index_cache.keys():
                size = Path(sst_path).stat().st_size
                print(f"    - {Path(sst_path).name} ({size} bytes)")
        print()
    
    def cmd_keys(self, args):
        """KEYS - List all keys in memtable"""
        items = self.engine.memtable.get_sorted_items()
        if not items:
            self.info("(empty memtable)")
            return
        
        print(f"\n\033[1;33mKeys in memtable ({len(items)}):\033[0m")
        for key, _ in items:
            print(f"  • {key}")
        print()
    
    def cmd_help(self, args):
        """HELP - Show help"""
        print(self.HELP_TEXT)
    
    def cmd_clear(self, args):
        """CLEAR - Clear screen"""
        print("\033[2J\033[H", end="")
    
    def cmd_exit(self, args):
        """EXIT - Exit CLI"""
        self.running = False
        self.engine.wal.close()
        print("\n\033[1;35mGoodbye! \033[0m\n")
    
    def process_command(self, line):
        """Process a single command"""
        line = line.strip()
        if not line:
            return
        
        parts = line.split(maxsplit=1)
        cmd = parts[0].upper()
        args = parts[1].split() if len(parts) > 1 else []
        
        # For SET, preserve the full value string
        if cmd == "SET" and len(parts) > 1:
            # Re-parse to handle value with spaces
            set_parts = parts[1].split(maxsplit=1)
            args = set_parts if len(set_parts) > 0 else []
        
        commands = {
            "SET": self.cmd_set,
            "GET": self.cmd_get,
            "DEL": self.cmd_del,
            "FLUSH": self.cmd_flush,
            "STATS": self.cmd_stats,
            "KEYS": self.cmd_keys,
            "HELP": self.cmd_help,
            "CLEAR": self.cmd_clear,
            "EXIT": self.cmd_exit,
            "QUIT": self.cmd_exit,
        }
        
        if cmd in commands:
            try:
                commands[cmd](args)
            except Exception as e:
                self.error(str(e))
        else:
            self.error(f"Unknown command: {cmd}. Type HELP for available commands.")
    
    def run(self):
        """Main REPL loop"""
        self.print_banner()
        
        while self.running:
            try:
                line = input(self.PROMPT)
                self.process_command(line)
            except KeyboardInterrupt:
                print("\n\033[1;33m(Use EXIT or QUIT to leave)\033[0m")
            except EOFError:
                self.cmd_exit([])


def main():
    """Entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="py-lsm: LSM-Tree based Key-Value Store"
    )
    parser.add_argument(
        "--db", "-d",
        default="lsm_db",
        help="Database folder path (default: lsm_db)"
    )
    parser.add_argument(
        "--capacity", "-c",
        type=int,
        default=100,
        help="Memtable capacity before flush (default: 100)"
    )
    
    args = parser.parse_args()
    
    cli = CLI(db_folder=args.db, capacity=args.capacity)
    cli.run()


if __name__ == "__main__":
    main()
