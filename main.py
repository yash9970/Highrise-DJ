import os
import sys

# Force Render to print all logs instantly
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

if __name__ == "__main__":
    # Point to the actual bot directory
    bot_dir = os.path.join(os.path.dirname(__file__), "artifacts", "dj-bot")
    
    # Add it to the Python path
    sys.path.insert(0, bot_dir)
    
    # Change current working directory so relative paths work (like db or .env)
    os.chdir(bot_dir)
    
    # Import the main bot script and run it
    import main as bot_main
    import asyncio
    
    try:
        asyncio.run(bot_main.run_all())
    except KeyboardInterrupt:
        print("Bot stopped by user.")
