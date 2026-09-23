import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

REQUIRED = [
    "DISCORD_TOKEN",
    "CLIENT_ID",
    "GUILD_ID",
    "OWNER_ID",
    "OTP_CHANNEL_ID",
    "SMSPOOL_API_KEY",
]
missing = [name for name in REQUIRED if not os.getenv(name)]
if missing:
    raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

TOKEN = os.environ["DISCORD_TOKEN"]
CLIENT_ID = os.environ["CLIENT_ID"]
GUILD_ID = int(os.environ["GUILD_ID"])
OWNER_ID = os.environ["OWNER_ID"]
OTP_CHANNEL_ID = int(os.environ["OTP_CHANNEL_ID"])

SMSPOOL_API_KEY = os.environ["SMSPOOL_API_KEY"]
SMSPOOL_API_BASE = os.getenv("SMSPOOL_API_BASE", "https://api.smspool.net").rstrip("/")
DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "62")
DEFAULT_SERVICE = os.getenv("DEFAULT_SERVICE", "whatsapp")
MAX_DAILY_ORDERS = int(os.getenv("MAX_DAILY_ORDERS", "10"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
ORDER_TIMEOUT_SECONDS = int(os.getenv("ORDER_TIMEOUT_SECONDS", "600"))
PERMISSIONS_FILE = Path("authorized_users.json")


def load_authorized_users() -> set[str]:
    if not PERMISSIONS_FILE.exists():
        return set()
    try:
        data = json.loads(PERMISSIONS_FILE.read_text(encoding="utf-8"))
        return {str(user_id) for user_id in data if str(user_id).isdigit()}
    except (OSError, json.JSONDecodeError):
        return set()


def save_authorized_users(user_ids: set[str]) -> None:
    PERMISSIONS_FILE.write_text(
        json.dumps(sorted(user_ids), indent=2),
        encoding="utf-8",
    )


def safe_text(value: object) -> str:
    return str(value or "").replace("@everyone", "@ everyone").replace("@here", "@ here")


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class SMSPool:
    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    async def post(self, endpoint: str, fields: dict[str, object]) -> dict:
        if not self.session:
            raise RuntimeError("SMSPool client is not started")

        data = {"key": SMSPOOL_API_KEY, **fields}
        async with self.session.post(f"{SMSPOOL_API_BASE}{endpoint}", data=data) as response:
            raw = await response.text()
            try:
                payload = await response.json(content_type=None)
            except Exception as exc:
                raise RuntimeError(f"SMSPool returned non-JSON ({response.status})") from exc

        if response.status >= 400 or str(payload.get("success", "1")) == "0":
            errors = payload.get("errors", [])
            if isinstance(errors, list):
                detail = "; ".join(
                    safe_text(item.get("message") or item.get("description") or item)
                    if isinstance(item, dict)
                    else safe_text(item)
                    for item in errors
                )
            else:
                detail = safe_text(payload.get("message") or raw or f"HTTP {response.status}")
            raise RuntimeError(detail)
        return payload

    async def purchase(self, country: str, service: str) -> dict:
        # SMSPool's advanced purchase endpoint accepts a country ID and service name/ID.
        return await self.post("/purchase/sms", {"country": country, "service": service})

    async def check_sms(self, order_id: str) -> dict:
        return await self.post("/sms/check", {"orderid": order_id})

    async def cancel_order(self, order_id: str) -> dict:
        return await self.post("/sms/cancel", {"orderid": order_id})

    async def balance(self) -> dict:
        return await self.post("/request/balance", {})


class OwnerOnlyBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        super().__init__(command_prefix="!", intents=intents)
        self.sms = SMSPool()
        self.authorized_users = load_authorized_users()
        self.active_order: Optional[dict] = None
        self.order_task: Optional[asyncio.Task] = None
        self.daily_count = 0
        self.daily_day = utc_day()

    async def setup_hook(self) -> None:
        await self.sms.start()
        guild = discord.Object(id=GUILD_ID)
        self.tree.copy_global_to(guild=guild)
        synced = await self.tree.sync(guild=guild)
        print(f"Synced {len(synced)} commands to guild {GUILD_ID}")

    async def close(self) -> None:
        await self.sms.close()
        await super().close()

    def reset_daily_count(self) -> None:
        today = utc_day()
        if today != self.daily_day:
            self.daily_day = today
            self.daily_count = 0

    async def guard(self, interaction: discord.Interaction) -> bool:
        user_id = str(interaction.user.id)
        if user_id != OWNER_ID and user_id not in self.authorized_users:
            await interaction.response.send_message(
                "❌ You are not authorized to use this bot.", ephemeral=True
            )
            return False
        if interaction.channel_id != OTP_CHANNEL_ID:
            await interaction.response.send_message(
                f"❌ Use this command in <#{OTP_CHANNEL_ID}>.", ephemeral=True
            )
            return False
        return True

    async def otp_channel(self) -> discord.abc.Messageable:
        channel = self.get_channel(OTP_CHANNEL_ID) or await self.fetch_channel(OTP_CHANNEL_ID)
        if not hasattr(channel, "send"):
            raise RuntimeError("OTP_CHANNEL_ID is not a writable text channel")
        return channel  # type: ignore[return-value]

    async def send_to_otp_channel(self, content: str) -> None:
        channel = await self.otp_channel()
        await channel.send(content, allowed_mentions=discord.AllowedMentions.none())

    async def refund_active_order(self) -> dict:
        if not self.active_order:
            raise RuntimeError("There is no active order to refund")

        order_id = str(self.active_order["order_id"])
        result = await self.sms.cancel_order(order_id)
        if self.order_task and self.order_task is not asyncio.current_task():
            self.order_task.cancel()
        self.active_order = None
        return result

    async def poll_for_otp(self, order_id: str, phone: str) -> None:
        started = asyncio.get_running_loop().time()
        while asyncio.get_running_loop().time() - started < ORDER_TIMEOUT_SECONDS:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            try:
                data = await self.sms.check_sms(order_id)
            except Exception as exc:
                print(f"SMSPool polling error for {order_id}: {exc}")
                continue

            status = int(data.get("status", 0) or 0)
            otp = data.get("sms")
            if not otp and data.get("full_sms"):
                matches = re.findall(r"(?<!\\d)\\d{4,8}(?!\\d)", str(data["full_sms"]))
                otp = matches[0] if matches else data["full_sms"]
            if status == 3 and otp:
                await self.send_to_otp_channel(
                    "\n".join(
                        [
                            "✅ **OTP received**",
                            f"**• Number:-** {safe_text(phone)}",
                            f"**• Otp:-** {safe_text(otp)}",
                        ]
                    )
                )
                return

        await self.send_to_otp_channel(
            "\n".join(
                [
                    "⌛ **OTP wait timed out**",
                    f"• Number: `{safe_text(phone)}`",
                    f"• Order ID: `{safe_text(order_id)}`",
                    "No SMS was received before the configured timeout.",
                ]
            )
        )


bot = OwnerOnlyBot()
guild = discord.Object(id=GUILD_ID)


async def owner_slash_guard(interaction: discord.Interaction) -> bool:
    if interaction.guild_id != GUILD_ID or str(interaction.user.id) != OWNER_ID:
        await interaction.response.send_message(
            "❌ Only the bot owner can manage permissions.", ephemeral=True
        )
        return False
    return True


@bot.tree.command(name="gp", description="Grant bot access to a server member", guild=guild)
@app_commands.describe(user="Member who should be allowed to use the bot")
async def grant_permission(interaction: discord.Interaction, user: discord.Member) -> None:
    if not await owner_slash_guard(interaction):
        return
    bot.authorized_users.add(str(user.id))
    save_authorized_users(bot.authorized_users)
    await interaction.response.send_message(
        f"✅ {user.mention} can now use `/buy`, `/balance`, `/status`, and `/refund`.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.tree.command(name="rp", description="Remove bot access from a server member", guild=guild)
@app_commands.describe(user="Member whose bot access should be removed")
async def revoke_permission(interaction: discord.Interaction, user: discord.Member) -> None:
    if not await owner_slash_guard(interaction):
        return
    if str(user.id) == OWNER_ID:
        await interaction.response.send_message(
            "❌ Owner permission cannot be removed.", ephemeral=True
        )
        return
    bot.authorized_users.discard(str(user.id))
    save_authorized_users(bot.authorized_users)
    await interaction.response.send_message(
        f"✅ Permission removed from {user.mention}.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.tree.command(name="perms", description="List users allowed to use the bot", guild=guild)
async def list_permissions(interaction: discord.Interaction) -> None:
    if not await owner_slash_guard(interaction):
        return
    if not bot.authorized_users:
        await interaction.response.send_message(
            "ℹ️ No extra users have permission.", ephemeral=True
        )
        return
    mentions = "\n".join(f"• <@{user_id}>" for user_id in sorted(bot.authorized_users))
    await interaction.response.send_message(
        f"**Users allowed to use the bot:**\n{mentions}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.tree.command(name="buy", description="Purchase one SMSPool number", guild=guild)
@app_commands.describe(
    country=f"SMSPool country ID; default {DEFAULT_COUNTRY}",
    service=f"SMSPool service name or ID; default {DEFAULT_SERVICE}",
)
async def buy(
    interaction: discord.Interaction,
    country: Optional[str] = None,
    service: Optional[str] = None,
) -> None:
    if not await bot.guard(interaction):
        return

    bot.reset_daily_count()
    if bot.active_order:
        await interaction.response.send_message(
            "⏳ An order is already active. Wait for it to finish.", ephemeral=True
        )
        return
    if bot.daily_count >= MAX_DAILY_ORDERS:
        await interaction.response.send_message(
            "🛑 Daily purchase limit reached.", ephemeral=True
        )
        return

    country = country or DEFAULT_COUNTRY
    service = service or DEFAULT_SERVICE
    await interaction.response.defer(ephemeral=True)

    try:
        data = await bot.sms.purchase(country, service)
        order_id = data.get("order_id") or data.get("orderid")
        phone = data.get("phonenumber") or data.get("phone_number")
        if not order_id or not phone:
            raise RuntimeError("SMSPool did not return an order ID and phone number")

        bot.active_order = {
            "order_id": str(order_id),
            "phone": str(phone),
            "country": country,
            "service": service,
            "started_at": asyncio.get_running_loop().time(),
        }
        bot.daily_count += 1

        country_display = safe_text(data.get("cc") or country)
        if country_display.isdigit():
            country_display = f"+{country_display}"
        expiry = f"{data['expires_in']}s" if data.get("expires_in") else "SMSPool default"
        await bot.send_to_otp_channel(
            "\n".join(
                [
                    "📱 **SMSPool number purchased**",
                    f"**• Country:-** {country_display}",
                    f"**• Service:-** {safe_text(service)}",
                    f"**• Number:-** {safe_text(phone)}",
                    f"**• Expires in:-** {safe_text(expiry)}",
                    "",
                    "**Waiting for the OTP…**",
                ]
            )
        )
        await interaction.edit_original_response(
            content=f"✅ Number purchased and posted in <#{OTP_CHANNEL_ID}>."
        )

        async def monitor() -> None:
            try:
                await bot.poll_for_otp(str(order_id), str(phone))
            except Exception as exc:
                print(f"OTP polling stopped for {order_id}: {exc}")
                try:
                    await bot.send_to_otp_channel(
                        f"⚠️ OTP polling error for order `{safe_text(order_id)}`: {safe_text(exc)}"
                    )
                except Exception as channel_exc:
                    print(f"Could not post polling error: {channel_exc}")
            finally:
                if bot.active_order and bot.active_order["order_id"] == str(order_id):
                    bot.active_order = None
                bot.order_task = None

        bot.order_task = asyncio.create_task(monitor())
    except Exception as exc:
        await interaction.edit_original_response(content=f"❌ {safe_text(exc)}")


@bot.tree.command(name="balance", description="Show the SMSPool balance", guild=guild)
async def balance(interaction: discord.Interaction) -> None:
    if not await bot.guard(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        data = await bot.sms.balance()
        await interaction.edit_original_response(
            content=f"💰 SMSPool balance: `{safe_text(data.get('balance', 'unknown'))}`"
        )
    except Exception as exc:
        await interaction.edit_original_response(content=f"❌ {safe_text(exc)}")


@bot.tree.command(name="status", description="Show the active order status", guild=guild)
async def status(interaction: discord.Interaction) -> None:
    if not await bot.guard(interaction):
        return
    if not bot.active_order:
        await interaction.response.send_message("✅ No active order.", ephemeral=True)
        return

    age = int(asyncio.get_running_loop().time() - bot.active_order["started_at"])
    await interaction.response.send_message(
        "\n".join(
            [
                "📡 **Active order**",
                f"• Number: `{safe_text(bot.active_order['phone'])}`",
                f"• Order ID: `{safe_text(bot.active_order['order_id'])}`",
                f"• Age: `{age}s`",
            ]
        ),
        ephemeral=True,
    )


@bot.tree.command(name="refund", description="Cancel the active order and request a refund", guild=guild)
@app_commands.describe(confirm="Set true only when no OTP was received")
async def refund(interaction: discord.Interaction, confirm: bool = False) -> None:
    if not await bot.guard(interaction):
        return
    if not confirm:
        await interaction.response.send_message(
            "Use `/refund confirm:true` only when the SMS did not arrive or did not work.",
            ephemeral=True,
        )
        return
    if not bot.active_order:
        await interaction.response.send_message("❌ No active order to refund.", ephemeral=True)
        return

    order = bot.active_order.copy()
    await interaction.response.defer(ephemeral=True)
    try:
        await bot.refund_active_order()
        await bot.send_to_otp_channel(
            "\n".join(
                [
                    "↩️ **Refund requested**",
                    f"**• Number:-** {safe_text(order['phone'])}",
                    f"**• Order ID:-** {safe_text(order['order_id'])}",
                    "SMSPool cancelled the active order and will return the eligible amount.",
                ]
            )
        )
        await interaction.edit_original_response(content="✅ Refund request sent to SMSPool.")
    except Exception as exc:
        await interaction.edit_original_response(content=f"❌ {safe_text(exc)}")


@bot.event
async def on_ready() -> None:
    print(f"Logged in as {bot.user}")
    print(f"Owner-only mode enabled for Discord user {OWNER_ID}")
    print(f"OTP output channel: {OTP_CHANNEL_ID}")


async def main() -> None:
    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
