import os
import tempfile
import sys
import atexit
from typing import Optional, Tuple, Set

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands

# -------------------------
# Options
# -------------------------
PROGRAMS = ["temp", "perm", "private"]
DURATIONS = ["day", "week", "month", "lifetime"]

# -------------------------
# Paths (Railway Volume ready)
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Railway: set DATA_DIR=/app/data and mount a Volume there
DATA_DIR = os.getenv("DATA_DIR", os.path.join(BASE_DIR, "data"))
STOCK_DIR = os.path.join(DATA_DIR, "stock")
DB_PATH = os.path.join(DATA_DIR, "bot.db")

# -------------------------
# Embed / Branding
# -------------------------
THEME_COLOR = int("07bbac", 16)  # #07bbac
BANNER_URL = "https://cdn.discordapp.com/attachments/1448802836195442728/1457111666234757337/banner_1.jpg?ex=695ad07d&is=69597efd&hm=de18b8b085db4d98fe138e92749a56f7f9a753e5cd97b89114bcf4caaa2c885a&"
SUPPORT_SERVER_URL = "https://discord.com/channels/1377534339151171614/1377717515018440805"

# Role name to assign to resellers
RESELLER_ROLE_NAME = "Reseller"

# -------------------------
# Logging
# -------------------------
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "1377544383154360340"))
LOG_FULL_KEY = os.getenv("LOG_FULL_KEY", "false").lower() in ("1", "true", "yes", "y")

# -------------------------
# Config via ENV (Railway-friendly)
# -------------------------
def parse_owner_ids(raw: str) -> Set[int]:
    out: Set[int] = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
OWNER_IDS = parse_owner_ids(os.getenv("OWNER_IDS", ""))

def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS

# -------------------------
# Bot (create early so helpers can reference it)
# -------------------------
intents = discord.Intents.default()
intents.members = True  # Enable "Server Members Intent" in Dev Portal
bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------
# Single-instance lock (optional; disable on Railway)
# -------------------------
LOCK_ENABLED = os.getenv("LOCK_ENABLED", "false").lower() in ("1", "true", "yes", "y")
LOCK_PATH = os.path.join(tempfile.gettempdir(), "bot.lock")

def acquire_lock():
    if not LOCK_ENABLED:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.close(fd)
    except FileExistsError:
        print("Another bot instance is already running. Exiting.")
        sys.exit(1)

def release_lock():
    if not LOCK_ENABLED:
        return
    try:
        os.remove(LOCK_PATH)
    except FileNotFoundError:
        pass

acquire_lock()
atexit.register(release_lock)

# -------------------------
# Helpers
# -------------------------
def stock_file(program: str, duration: str) -> str:
    return os.path.join(STOCK_DIR, f"{program}_{duration}.txt")

def choiceify(values):
    return [app_commands.Choice(name=v, value=v) for v in values]

def mask_key(k: str) -> str:
    k = (k or "").strip()
    if len(k) <= 8:
        return k
    return f"{k[:4]}{'*' * (len(k) - 8)}{k[-4:]}"

def shorten_error(msg: str, limit: int = 300) -> str:
    msg = (msg or "").strip()
    if len(msg) <= limit:
        return msg
    return msg[:limit - 3] + "..."

# -------------------------
# DB init
# -------------------------
async def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(STOCK_DIR, exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                program TEXT NOT NULL,
                duration TEXT NOT NULL,
                key TEXT NOT NULL UNIQUE,
                used_by INTEGER,
                used_for INTEGER,
                used_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS resellers (
                user_id INTEGER NOT NULL,
                program TEXT NOT NULL,
                PRIMARY KEY (user_id, program)
            )
        """)
        await db.commit()

# -------------------------
# Reseller helpers
# -------------------------
async def reseller_has_access(user_id: int, program: str) -> bool:
    if is_owner(user_id):
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM resellers WHERE user_id = ? AND program = ?",
            (user_id, program),
        )
        row = await cur.fetchone()
        return row is not None

async def is_reseller_anywhere(user_id: int) -> bool:
    if is_owner(user_id):
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM resellers WHERE user_id = ? LIMIT 1", (user_id,))
        row = await cur.fetchone()
        return row is not None

async def add_reseller_db(user_id: int, program: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO resellers(user_id, program) VALUES(?, ?)",
            (user_id, program),
        )
        await db.commit()

async def remove_reseller_db(user_id: int, program: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM resellers WHERE user_id = ? AND program = ?",
            (user_id, program),
        )
        await db.commit()

# -------------------------
# Stock helpers
# -------------------------
async def add_stock_via_text(program: str, duration: str, raw_text: str) -> Tuple[int, int]:
    """
    Inserts into DB first.
    Only appends to TXT for keys that actually inserted.
    Accepts separators: newline, comma, semicolon, tab, space
    Returns (added, skipped_duplicates)
    """
    path = stock_file(program, duration)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    normalized = (
        raw_text.replace(",", "\n")
        .replace(";", "\n")
        .replace("\t", "\n")
        .replace(" ", "\n")
    )

    seen = set()
    keys = []
    for line in normalized.splitlines():
        k = line.strip()
        if k and k not in seen:
            seen.add(k)
            keys.append(k)

    if not keys:
        return 0, 0

    added_keys = []
    skipped = 0

    async with aiosqlite.connect(DB_PATH) as db:
        for k in keys:
            try:
                await db.execute(
                    "INSERT INTO keys(program, duration, key) VALUES(?, ?, ?)",
                    (program, duration, k),
                )
                added_keys.append(k)
            except aiosqlite.IntegrityError:
                skipped += 1
        await db.commit()

    if added_keys:
        with open(path, "a", encoding="utf-8") as f:
            for k in added_keys:
                f.write(k + "\n")

    return len(added_keys), skipped

async def get_stock_count(program: str, duration: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM keys WHERE program = ? AND duration = ? AND used_at IS NULL",
            (program, duration),
        )
        (count,) = await cur.fetchone()
        return int(count)

def remove_key_from_txt(program: str, duration: str, key_value: str) -> bool:
    """
    Removes ONE occurrence of key_value from the txt file.
    """
    path = stock_file(program, duration)
    if not os.path.exists(path):
        return False

    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    removed = False
    new_lines = []
    for line in lines:
        if (not removed) and line.strip() == key_value:
            removed = True
            continue
        new_lines.append(line)

    if removed:
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(new_lines).rstrip() + ("\n" if new_lines else ""))

    return removed

async def pop_key(program: str, duration: str, reseller_id: int, buyer_id: int) -> Optional[str]:
    """
    Atomically claims one unused key and marks it used.
    Then removes that key from the TXT.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")

        cur = await db.execute(
            "SELECT id, key FROM keys WHERE program = ? AND duration = ? AND used_at IS NULL LIMIT 1",
            (program, duration),
        )
        row = await cur.fetchone()
        if not row:
            await db.execute("ROLLBACK")
            return None

        key_id, key_value = row

        await db.execute(
            "UPDATE keys SET used_by = ?, used_for = ?, used_at = datetime('now') WHERE id = ?",
            (reseller_id, buyer_id, key_id),
        )
        await db.commit()

    try:
        ok = remove_key_from_txt(program, duration, key_value)
        if not ok:
            print(f"[WARN] Key not found in TXT for {program}_{duration}: {key_value}")
    except Exception as e:
        print(f"[WARN] Failed to remove key from TXT: {type(e).__name__}: {e}")

    return key_value

# -------------------------
# Logging
# -------------------------
async def get_log_channel() -> Optional[discord.abc.Messageable]:
    ch = bot.get_channel(LOG_CHANNEL_ID)
    if ch is not None:
        return ch
    try:
        return await bot.fetch_channel(LOG_CHANNEL_ID)
    except Exception as e:
        print(f"[LOG] fetch_channel failed: {type(e).__name__}: {e}")
        return None

async def log_gen_key(
    program: str,
    duration: str,
    key_value: str,
    reseller: discord.abc.User,
    buyer: discord.abc.User,
    dm_sent: bool,
    dm_error: Optional[str],
):
    channel = await get_log_channel()
    if channel is None:
        return

    shown_key = key_value if LOG_FULL_KEY else mask_key(key_value)

    embed = discord.Embed(title="Key Generated", color=THEME_COLOR)
    embed.add_field(name="Program", value=program, inline=True)
    embed.add_field(name="Duration", value=duration, inline=True)
    embed.add_field(name="Key", value=f"```{shown_key}```", inline=False)
    embed.add_field(name="Buyer", value=f"{buyer.mention} (`{buyer.id}`)", inline=False)
    embed.add_field(name="Reseller", value=f"{reseller.mention} (`{reseller.id}`)", inline=False)
    embed.add_field(name="DM Sent", value="Yes ✅" if dm_sent else "No ❌", inline=True)

    if not dm_sent and dm_error:
        embed.add_field(name="DM Error", value=f"```{shorten_error(dm_error)}```", inline=False)

    embed.timestamp = discord.utils.utcnow()

    try:
        await channel.send(embed=embed)
    except Exception as e:
        print(f"[LOG] send failed: {type(e).__name__}: {e}")

# -------------------------
# DM Buttons
# -------------------------
class LicenseDMView(discord.ui.View):
    def __init__(self, key_value: str):
        super().__init__(timeout=None)
        self.key_value = key_value

        self.add_item(discord.ui.Button(
            label="Support",
            style=discord.ButtonStyle.link,
            url=SUPPORT_SERVER_URL
        ))

    @discord.ui.button(label="Copy Key", style=discord.ButtonStyle.primary)
    async def copy_key(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(f"```{self.key_value}```", ephemeral=True)

        button.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

# -------------------------
# Owner DM helper
# -------------------------
async def dm_owners(embed: discord.Embed):
    for owner_id in OWNER_IDS:
        user = bot.get_user(owner_id)
        if user is None:
            try:
                user = await bot.fetch_user(owner_id)
            except Exception:
                continue
        try:
            await user.send(embed=embed)
        except Exception:
            pass

# -------------------------
# Events
# -------------------------
@bot.event
async def on_ready():
    await init_db()
    try:
        synced = await bot.tree.sync()
        print(f"Logged in as {bot.user} | Synced {len(synced)} commands")
        print(f"[PATHS] DATA_DIR={DATA_DIR} DB_PATH={DB_PATH} STOCK_DIR={STOCK_DIR}")
    except Exception as e:
        print("Sync error:", e)

# -------------------------
# Commands
# -------------------------
@bot.tree.command(name="add_reseller", description="Owner: allow a user to sell/generate keys for a program")
@app_commands.describe(program="temp / perm / private", user="The reseller")
@app_commands.choices(program=choiceify(PROGRAMS))
async def add_reseller_cmd(interaction: discord.Interaction, program: app_commands.Choice[str], user: discord.Member):
    if not is_owner(interaction.user.id):
        return await interaction.response.send_message("Owner only.", ephemeral=True)

    await add_reseller_db(user.id, program.value)

    if interaction.guild is None:
        return await interaction.response.send_message(
            "Added reseller in DB, but I can't assign roles in DMs.",
            ephemeral=True
        )

    role = discord.utils.get(interaction.guild.roles, name=RESELLER_ROLE_NAME)
    if role is None:
        return await interaction.response.send_message(
            f"Added reseller in DB, but I couldn't find a role named '{RESELLER_ROLE_NAME}'. Create it or rename it.",
            ephemeral=True
        )

    role_note = ""
    try:
        await user.add_roles(role, reason="Added as reseller")
        role_note = f" and gave **{role.name}** role"
    except discord.Forbidden:
        role_note = " (but I could not assign the role: check Manage Roles + role hierarchy)"
    except Exception:
        role_note = " (but I could not assign the role due to an error)"

    await interaction.response.send_message(
        f"Added {user.mention} as reseller for **{program.value}**{role_note}.",
        ephemeral=True,
    )

@bot.tree.command(name="remove_reseller", description="Owner: remove reseller access for a program")
@app_commands.describe(program="temp / perm / private", user="The reseller")
@app_commands.choices(program=choiceify(PROGRAMS))
async def remove_reseller_cmd(interaction: discord.Interaction, program: app_commands.Choice[str], user: discord.Member):
    if not is_owner(interaction.user.id):
        return await interaction.response.send_message("Owner only.", ephemeral=True)

    await remove_reseller_db(user.id, program.value)

    still_reseller = await is_reseller_anywhere(user.id)

    role_note = ""
    if interaction.guild is not None:
        role = discord.utils.get(interaction.guild.roles, name=RESELLER_ROLE_NAME)
        if role is None:
            role_note = f" (Role '{RESELLER_ROLE_NAME}' not found, so I couldn't remove it.)"
        else:
            if still_reseller:
                role_note = " (They still have reseller access for another program, so I kept the role.)"
            else:
                try:
                    await user.remove_roles(role, reason="Removed as reseller")
                    role_note = f" and removed **{role.name}** role"
                except discord.Forbidden:
                    role_note = " (But I couldn't remove the role: check Manage Roles + role hierarchy.)"
                except Exception:
                    role_note = " (But I couldn't remove the role due to an error.)"
    else:
        role_note = " (No guild context; couldn't remove role.)"

    await interaction.response.send_message(
        f"Removed {user.mention} from **{program.value}** resellers{role_note}.",
        ephemeral=True,
    )

@bot.tree.command(name="add_stock_file", description="Owner: upload a .txt file to add stock")
@app_commands.describe(
    program="temp / perm / private",
    duration="day / week / month / lifetime",
    file="Upload a .txt with 1 key per line",
)
@app_commands.choices(program=choiceify(PROGRAMS), duration=choiceify(DURATIONS))
async def add_stock_file_cmd(interaction: discord.Interaction, program: app_commands.Choice[str], duration: app_commands.Choice[str], file: discord.Attachment):
    if not is_owner(interaction.user.id):
        return await interaction.response.send_message("Owner only.", ephemeral=True)

    if not file.filename.lower().endswith(".txt"):
        return await interaction.response.send_message("Upload a .txt file.", ephemeral=True)

    data = await file.read()
    text = data.decode("utf-8", errors="ignore")

    added, skipped = await add_stock_via_text(program.value, duration.value, text)

    await interaction.response.send_message(
        f"Added from file **{file.filename}** to **{program.value} {duration.value}**\n"
        f"✅ Added: **{added}**\n"
        f"⚠️ Duplicates skipped: **{skipped}**",
        ephemeral=True,
    )

@bot.tree.command(name="clear_stock", description="Owner: clear ALL stock for a program + duration (wipes txt too)")
@app_commands.describe(program="temp / perm / private", duration="day / week / month / lifetime")
@app_commands.choices(program=choiceify(PROGRAMS), duration=choiceify(DURATIONS))
async def clear_stock_cmd(interaction: discord.Interaction, program: app_commands.Choice[str], duration: app_commands.Choice[str]):
    if not is_owner(interaction.user.id):
        return await interaction.response.send_message("Owner only.", ephemeral=True)

    prog = program.value
    dur = duration.value

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM keys WHERE program = ? AND duration = ? AND used_at IS NULL",
            (prog, dur),
        )
        (before_count,) = await cur.fetchone()

        await db.execute(
            "DELETE FROM keys WHERE program = ? AND duration = ? AND used_at IS NULL",
            (prog, dur),
        )
        await db.commit()

    # wipe txt
    path = stock_file(prog, dur)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("")

    await interaction.response.send_message(
        f"Cleared stock for **{prog} {dur}**.\n"
        f"Removed **{int(before_count)}** unused keys from DB and wiped the txt.",
        ephemeral=True,
    )

@bot.tree.command(name="stock", description="Check unused stock count for a program/duration")
@app_commands.describe(program="temp / perm / private", duration="day / week / month / lifetime")
@app_commands.choices(program=choiceify(PROGRAMS), duration=choiceify(DURATIONS))
async def stock_cmd(interaction: discord.Interaction, program: app_commands.Choice[str], duration: app_commands.Choice[str]):
    if not await reseller_has_access(interaction.user.id, program.value):
        return await interaction.response.send_message("You don’t have access to that program.", ephemeral=True)

    count = await get_stock_count(program.value, duration.value)
    await interaction.response.send_message(
        f"Stock for **{program.value} {duration.value}**: **{count}** unused keys.",
        ephemeral=True,
    )

@bot.tree.command(name="hwid_reset", description="Request a HWID reset (DMs the owner)")
@app_commands.describe(key="Customer key", duration="day / week / month / lifetime", reason="Reason for reset")
@app_commands.choices(duration=choiceify(DURATIONS))
async def hwid_reset_cmd(interaction: discord.Interaction, key: str, duration: app_commands.Choice[str], reason: str):
    if not await is_reseller_anywhere(interaction.user.id):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)

    embed = discord.Embed(title="HWID Reset Request", color=THEME_COLOR)
    embed.add_field(name="Key", value=f"```{key}```", inline=False)
    embed.add_field(name="Duration", value=duration.value, inline=True)
    embed.add_field(name="Reason", value=reason, inline=False)
    embed.add_field(name="Reseller", value=f"{interaction.user} ({interaction.user.id})", inline=False)
    embed.timestamp = discord.utils.utcnow()

    await dm_owners(embed)
    await interaction.response.send_message("Sent HWID reset request to the owner.", ephemeral=True)

@bot.tree.command(name="gen_key", description="Generate 1 key for a buyer (pulls from stock)")
@app_commands.describe(program="temp / perm / private", duration="day / week / month / lifetime", buyer="Who bought the key")
@app_commands.choices(program=choiceify(PROGRAMS), duration=choiceify(DURATIONS))
async def gen_key_cmd(interaction: discord.Interaction, program: app_commands.Choice[str], duration: app_commands.Choice[str], buyer: discord.Member):
    if not await reseller_has_access(interaction.user.id, program.value):
        return await interaction.response.send_message("You don’t have access to that program.", ephemeral=True)

    # ✅ Prevent "application did not respond"
    # If defer fails -> abort (no key popped, no DM)
    try:
        await interaction.response.defer(ephemeral=True, thinking=True)
    except Exception as e:
        print(f"[GEN_KEY] Defer failed -> abort (no key popped, no DM): {type(e).__name__}: {e}")
        return

    key_value = await pop_key(program.value, duration.value, interaction.user.id, buyer.id)
    if not key_value:
        return await interaction.followup.send(
            f"No stock left for **{program.value} {duration.value}**.",
            ephemeral=True,
        )

    dm_sent = False
    dm_error = None

    try:
        embed = discord.Embed(
            title="Your License Key",
            description="Thanks for your purchase!",
            color=THEME_COLOR,
        )
        embed.add_field(name="Program", value=program.value.capitalize(), inline=True)
        embed.add_field(name="Duration", value=duration.value.capitalize(), inline=True)
        embed.add_field(name="Key", value=f"```{key_value}```", inline=False)
        embed.set_footer(text="Keep this key private. Use the buttons below if you need help.")
        embed.set_image(url=BANNER_URL)

        view = LicenseDMView(key_value)

        # "Confirmed" if no exception
        await buyer.send(embed=embed, view=view)
        dm_sent = True

    except Exception as e:
        dm_error = f"{type(e).__name__}: {e}"
        print("[DM] Failed:", dm_error)

    await log_gen_key(
        program.value,
        duration.value,
        key_value,
        interaction.user,
        buyer,
        dm_sent,
        dm_error
    )

    if dm_sent:
        msg = f"✅ Pulled 1 key for {buyer.mention} (**{program.value} {duration.value}**) and confirmed DM sent."
    else:
        msg = (
            f"⚠️ Pulled 1 key for {buyer.mention} (**{program.value} {duration.value}**) but DM failed.\n"
            f"Reason: `{shorten_error(dm_error or 'Unknown')}`\n"
            f"Copy this key:\n`{key_value}`"
        )

    await interaction.followup.send(msg, ephemeral=True)

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Missing DISCORD_TOKEN env var. (Railway Variables)")
    if not OWNER_IDS:
        print("[WARN] OWNER_IDS is empty. Owner-only commands will not work.")
    bot.run(DISCORD_TOKEN)
