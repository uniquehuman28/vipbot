import asyncio
import os
import re
import time
import uuid
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
import psutil
import ping3
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

BOT_TOKEN = "7647013351:AAGq37YXun27mG-ypvWINmVaVHRVVYolTI4"
ADMIN_IDS = [7818451398, 7935274205]
MEMBER_IDS = [111111111, 222222222, 333333333] + ADMIN_IDS

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

processing_queue = asyncio.Queue()
current_processing = None
user_cache = {}

class States(StatesGroup):
    waiting_txt_files = State()
    waiting_vcf_files = State()
    waiting_batch_size = State()
    waiting_member_id = State()
    waiting_contact_prefix = State()
    waiting_output_filename = State()
    waiting_remove_member_id = State()

kode_negara = {
    "+62": {"negara": "Indonesia 🇮🇩", "pattern": r"(\+62)(\d{3,4})(\d+)", "format": "{0} {1} {2}"},
    "+1": {"negara": "United States 🇺🇸", "pattern": r"(\+1)(\d{3})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+44": {"negara": "United Kingdom 🇬🇧", "pattern": r"(\+44)(\d{4})(\d{6})", "format": "{0} {1} {2}"},
    "+91": {"negara": "India 🇮🇳", "pattern": r"(\+91)(\d{5})(\d{5})", "format": "{0} {1} {2}"},
    "+81": {"negara": "Japan 🇯🇵", "pattern": r"(\+81)(\d{2,4})(\d+)", "format": "{0} {1} {2}"},
    "+49": {"negara": "Germany 🇩🇪", "pattern": r"(\+49)(\d{3,4})(\d+)", "format": "{0} {1} {2}"},
    "+33": {"negara": "France 🇫🇷", "pattern": r"(\+33)(\d{1})(\d{2})(\d{2})(\d{2})(\d{2})", "format": "{0} {1} {2} {3} {4} {5}"},
    "+60": {"negara": "Malaysia 🇲🇾", "pattern": r"(\+60)(\d{2,3})(\d+)", "format": "{0} {1} {2}"},
    "+65": {"negara": "Singapore 🇸🇬", "pattern": r"(\+65)(\d{4})(\d{4})", "format": "{0} {1} {2}"},
    "+63": {"negara": "Philippines 🇵🇭", "pattern": r"(\+63)(\d{3})(\d+)", "format": "{0} {1} {2}"},
    "+234": {"negara": "Nigeria 🇳🇬", "pattern": r"(\+234)(\d{3})(\d+)", "format": "{0} {1} {2}"},
    "+86": {"negara": "China 🇨🇳", "pattern": r"(\+86)(\d{3})(\d{4})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+7": {"negara": "Russia 🇷🇺", "pattern": r"(\+7)(\d{3})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+55": {"negara": "Brazil 🇧🇷", "pattern": r"(\+55)(\d{2})(\d{4,5})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+39": {"negara": "Italy 🇮🇹", "pattern": r"(\+39)(\d{3})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+34": {"negara": "Spain 🇪🇸", "pattern": r"(\+34)(\d{3})(\d{3})(\d{3})", "format": "{0} {1} {2} {3}"},
    "+31": {"negara": "Netherlands 🇳🇱", "pattern": r"(\+31)(\d{2})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+46": {"negara": "Sweden 🇸🇪", "pattern": r"(\+46)(\d{2,3})(\d+)", "format": "{0} {1} {2}"},
    "+47": {"negara": "Norway 🇳🇴", "pattern": r"(\+47)(\d{3})(\d{2})(\d{3})", "format": "{0} {1} {2} {3}"},
    "+45": {"negara": "Denmark 🇩🇰", "pattern": r"(\+45)(\d{2})(\d{2})(\d{2})(\d{2})", "format": "{0} {1} {2} {3} {4}"},
    "+41": {"negara": "Switzerland 🇨🇭", "pattern": r"(\+41)(\d{2})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+43": {"negara": "Austria 🇦🇹", "pattern": r"(\+43)(\d{3})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+32": {"negara": "Belgium 🇧🇪", "pattern": r"(\+32)(\d{2})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+420": {"negara": "Czech Republic 🇨🇿", "pattern": r"(\+420)(\d{3})(\d{3})(\d{3})", "format": "{0} {1} {2} {3}"},
    "+48": {"negara": "Poland 🇵🇱", "pattern": r"(\+48)(\d{3})(\d{3})(\d{3})", "format": "{0} {1} {2} {3}"},
    "+36": {"negara": "Hungary 🇭🇺", "pattern": r"(\+36)(\d{2})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+40": {"negara": "Romania 🇷🇴", "pattern": r"(\+40)(\d{3})(\d{3})(\d{3})", "format": "{0} {1} {2} {3}"},
    "+30": {"negara": "Greece 🇬🇷", "pattern": r"(\+30)(\d{3})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+90": {"negara": "Turkey 🇹🇷", "pattern": r"(\+90)(\d{3})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+852": {"negara": "Hong Kong 🇭🇰", "pattern": r"(\+852)(\d{4})(\d{4})", "format": "{0} {1} {2}"},
    "+886": {"negara": "Taiwan 🇹🇼", "pattern": r"(\+886)(\d{2})(\d{4})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+82": {"negara": "South Korea 🇰🇷", "pattern": r"(\+82)(\d{2})(\d{4})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+66": {"negara": "Thailand 🇹🇭", "pattern": r"(\+66)(\d{2})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"},
    "+84": {"negara": "Vietnam 🇻🇳", "pattern": r"(\+84)(\d{3})(\d{3})(\d{4})", "format": "{0} {1} {2} {3}"}
}

def format_number(number: str, default_cc: str = "+62") -> Optional[str]:
    if not number:
        return None
    
    number = re.sub(r'[^\d+]', '', number)
    
    if number.startswith('0'):
        number = default_cc + number[1:]
    elif not number.startswith('+'):
        number = default_cc + number
    
    digits = number.replace('+', '')
    if len(digits) < 8 or len(digits) > 15:
        return None
    
    for code, info in kode_negara.items():
        if number.startswith(code):
            match = re.match(info["pattern"], number)
            if match:
                return info["format"].format(*match.groups())
    
    return number

def create_main_menu(user_id: int) -> InlineKeyboardMarkup:
    keyboard = []
    
    if user_id in MEMBER_IDS:
        keyboard.extend([
            [InlineKeyboardButton(text="📄➡️📇 TXT to VCF", callback_data="txt2vcf")],
            [InlineKeyboardButton(text="📇➡️📄 VCF to TXT", callback_data="vcf2txt")],
            [InlineKeyboardButton(text="❓ Help", callback_data="help")]
        ])
    else:
        keyboard.extend([
            [InlineKeyboardButton(text="🔒 TXT to VCF (Member Only)", callback_data="premium_required")],
            [InlineKeyboardButton(text="📇➡️📄 VCF to TXT", callback_data="vcf2txt")],
            [InlineKeyboardButton(text="❓ Help", callback_data="help")]
        ])
    
    if user_id in ADMIN_IDS:
        keyboard.extend([
            [InlineKeyboardButton(text="⚙️ Admin Tools", callback_data="admin_tools")]
        ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def create_admin_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text="💻 Status VPS", callback_data="status_vps")],
        [InlineKeyboardButton(text="👥 Kelola Member", callback_data="manage_members")],
        [InlineKeyboardButton(text="🗑️ Hapus Cache User", callback_data="hapus_cache")],
        [InlineKeyboardButton(text="🗑️ Hapus Semua Cache", callback_data="hapus_semua_cache")],
        [InlineKeyboardButton(text="🔙 Back to Menu", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def create_member_management_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text="➕ Tambah Member", callback_data="add_member")],
        [InlineKeyboardButton(text="➖ Hapus Member", callback_data="remove_member")],
        [InlineKeyboardButton(text="📋 List Member", callback_data="list_members")],
        [InlineKeyboardButton(text="🔙 Back to Admin", callback_data="admin_tools")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def get_queue_position(user_id: int) -> int:
    queue_list = list(processing_queue._queue)
    for i, item in enumerate(queue_list):
        if item.get('user_id') == user_id:
            return i + 1
    return 0

async def process_txt_to_vcf(files_data: List[bytes], batch_size: int, user_id: int, contact_prefix: str = "Contact", output_filename: str = "contacts") -> Tuple[List[str], Dict]:
    start_time = time.time()
    all_numbers = []
    stats = {"valid": 0, "invalid": 0, "countries": {}}
    
    for file_data in files_data:
        lines = file_data.decode('utf-8', errors='ignore').splitlines()
        for line in lines:
            line = line.strip()
            if line:
                formatted = format_number(line)
                if formatted:
                    all_numbers.append(formatted)
                    stats["valid"] += 1
                    
                    for code, info in kode_negara.items():
                        if formatted.startswith(code):
                            country = info["negara"]
                            stats["countries"][country] = stats["countries"].get(country, 0) + 1
                            break
                    else:
                        stats["countries"]["🌏 Lainnya"] = stats["countries"].get("🌏 Lainnya", 0) + 1
                else:
                    stats["invalid"] += 1
    
    unique_numbers = list(set(all_numbers))
    stats["valid"] = len(unique_numbers)
    
    vcf_files = []
    for i in range(0, len(unique_numbers), batch_size):
        batch = unique_numbers[i:i+batch_size]
        vcf_content = "BEGIN:VCARD\nVERSION:3.0\n"
        
        for idx, number in enumerate(batch, start=1):
            uid = str(uuid.uuid4())
            name = f"{contact_prefix}{i + idx}"
            vcf_content += f"FN:{name}\n"
            vcf_content += f"N:{name};;;;\n"
            vcf_content += f"UID:{uid}\n"
            vcf_content += f"TEL:{number}\n"
            vcf_content += "END:VCARD\n"
            vcf_content += "BEGIN:VCARD\nVERSION:3.0\n"
        
        vcf_content = vcf_content.rstrip("BEGIN:VCARD\nVERSION:3.0\n")
        
        filename = f"{output_filename}_{i//batch_size + 1}.vcf"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(vcf_content)
        vcf_files.append(filename)
    
    elapsed = time.time() - start_time
    return vcf_files, {"stats": stats, "elapsed": elapsed, "total_files": len(files_data)}

async def process_vcf_to_txt(files_data: List[bytes]) -> str:
    all_contacts = []
    
    for file_data in files_data:
        content = file_data.decode('utf-8', errors='ignore')
        vcards = content.split('BEGIN:VCARD')
        
        for vcard in vcards[1:]:
            fn_match = re.search(r'FN:(.+)', vcard)
            tel_match = re.search(r'TEL:(.+)', vcard)
            
            if fn_match and tel_match:
                name = fn_match.group(1).strip()
                number = tel_match.group(1).strip()
                formatted_number = format_number(number)
                if formatted_number:
                    all_contacts.append(f"{name};{formatted_number}")
    
    filename = f"extracted_contacts_{int(time.time())}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(all_contacts))
    
    return filename

@dp.message(Command("start"))
async def start_handler(message: Message):
    user_id = message.from_user.id
    username = message.from_user.first_name or "User"
    
    welcome_text = f"🎉 Selamat datang {username}!\n\n"
    welcome_text += "🤖 Bot VCF Converter siap membantu Anda:\n"
    welcome_text += "📄➡️📇 Konversi TXT ke VCF\n"
    welcome_text += "📇➡️📄 Konversi VCF ke TXT\n\n"
    
    if user_id in MEMBER_IDS:
        welcome_text += "✅ Status: Member\n"
    else:
        welcome_text += "⭐ Upgrade ke Member untuk fitur premium!\n"
    
    await message.answer(welcome_text, reply_markup=create_main_menu(user_id))

@dp.message(Command("help"))
async def help_handler(message: Message):
    user_id = message.from_user.id
    help_text = "📚 **BANTUAN BOT VCF CONVERTER**\n\n"
    
    help_text += "👥 **Perintah Umum:**\n"
    help_text += "/start - Menu utama\n"
    help_text += "/help - Bantuan ini\n\n"
    
    if user_id in MEMBER_IDS:
        help_text += "⭐ **Fitur Member:**\n"
        help_text += "/txt2vcf - Konversi TXT ke VCF\n"
        help_text += "• Upload file .txt berisi nomor telepon\n"
        help_text += "• Tentukan jumlah kontak per file VCF\n"
        help_text += "• Dapatkan file VCF terorganisir\n\n"
    
    help_text += "📇 **VCF ke TXT:**\n"
    help_text += "/vcf2txt - Konversi VCF ke TXT\n"
    help_text += "• Upload file .vcf\n"
    help_text += "• Dapatkan file .txt dengan format Nama;Nomor\n\n"
    
    if user_id in ADMIN_IDS:
        help_text += "⚙️ **Admin Tools:**\n"
        help_text += "/status_vps - Cek status server\n"
        help_text += "/hapus_cache - Hapus cache user\n"
        help_text += "/hapus_semua_cache - Hapus semua cache\n"
    
    await message.answer(help_text, parse_mode="Markdown")

@dp.callback_query(F.data == "txt2vcf")
async def txt2vcf_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id not in MEMBER_IDS:
        await callback.answer("❌ Fitur ini khusus member!", show_alert=True)
        return
    
    if current_processing:
        position = await get_queue_position(user_id)
        if position > 0:
            await callback.message.answer(f"⏳ Anda dalam antrian posisi #{position}\n⏱ Estimasi tunggu: {position * 2} menit")
            return
    
    await state.set_state(States.waiting_txt_files)
    await callback.message.answer(
        "📄 **Upload file TXT** yang berisi nomor telepon\n\n"
        "📝 Format: 1 nomor per baris\n"
        "📁 Bisa upload multiple files sekaligus\n"
        "⚠️ Setelah upload selesai, ketik /done"
    )

@dp.callback_query(F.data == "vcf2txt")
async def vcf2txt_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_vcf_files)
    await callback.message.answer(
        "📇 **Upload file VCF** yang ingin dikonversi\n\n"
        "📁 Bisa upload multiple files sekaligus\n"
        "⚠️ Setelah upload selesai, ketik /done"
    )

@dp.callback_query(F.data == "premium_required")
async def premium_required_callback(callback: CallbackQuery):
    keyboard = [[InlineKeyboardButton(text="💬 DM Admin", url="https://t.me/your_admin_username")]]
    await callback.message.answer(
        "❌ Fitur ini khusus member\n\n"
        "💎 Keuntungan member:\n"
        "• Konversi TXT ke VCF unlimited\n"
        "• Batch processing cepat\n"
        "• Format nomor auto-detect 30+ negara\n"
        "• Deduplication otomatis\n\n"
        "📩 Hubungi admin untuk upgrade:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )

@dp.callback_query(F.data == "admin_tools")
async def admin_tools_callback(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Akses ditolak", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⚙️ **ADMIN CONTROL PANEL**\n\n"
        "Pilih opsi admin:",
        reply_markup=create_admin_menu(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "status_vps")
async def status_vps_callback(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Akses ditolak", show_alert=True)
        return
    
    try:
        ping_time = ping3.ping('8.8.8.8') or 0
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        boot_time = psutil.boot_time()
        uptime = datetime.now() - datetime.fromtimestamp(boot_time)
        
        status_text = f"💻 **STATUS VPS**\n\n"
        status_text += f"📡 Ping: {ping_time*1000:.0f}ms\n"
        status_text += f"⚙️ CPU: {cpu_percent:.1f}%\n"
        status_text += f"💾 RAM: {memory.percent:.1f}% ({memory.used//1024//1024}MB/{memory.total//1024//1024}MB)\n"
        status_text += f"💿 Storage: {disk.percent:.1f}% ({disk.used//1024//1024//1024}GB/{disk.total//1024//1024//1024}GB)\n"
        status_text += f"⏱ Uptime: {uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m\n"
        
        await callback.message.answer(status_text, parse_mode="Markdown")
        
    except Exception as e:
        await callback.message.answer(f"❌ Error getting VPS status: {str(e)}")

@dp.callback_query(F.data == "hapus_cache")
async def hapus_cache_callback(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Akses ditolak", show_alert=True)
        return
    
    user_id = callback.from_user.id
    if user_id in user_cache:
        del user_cache[user_id]
    
    await callback.answer("✅ Cache user berhasil dihapus", show_alert=True)

@dp.callback_query(F.data == "hapus_semua_cache")
async def hapus_semua_cache_callback(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Akses ditolak", show_alert=True)
        return
    
    user_cache.clear()
    await callback.answer("✅ Semua cache berhasil dihapus", show_alert=True)

@dp.callback_query(F.data == "manage_members")
async def manage_members_callback(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Akses ditolak", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👥 **KELOLA MEMBER**\n\n"
        f"📊 Total Member: {len(MEMBER_IDS)}\n"
        f"⚙️ Total Admin: {len(ADMIN_IDS)}\n\n"
        "Pilih aksi:",
        reply_markup=create_member_management_menu(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "add_member")
async def add_member_callback(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Akses ditolak", show_alert=True)
        return
    
    await state.set_state(States.waiting_member_id)
    await callback.message.answer(
        "➕ **TAMBAH MEMBER BARU**\n\n"
        "📝 Kirim User ID yang ingin ditambahkan sebagai member\n"
        "💡 Cara dapat User ID:\n"
        "   • Chat @userinfobot\n"
        "   • Chat @my_id_bot\n"
        "   • Forward pesan user ke @userinfobot\n\n"
        "❌ Ketik /cancel untuk batal"
    )

@dp.callback_query(F.data == "remove_member")
async def remove_member_callback(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Akses ditolak", show_alert=True)
        return
    
    if len(MEMBER_IDS) <= len(ADMIN_IDS):
        await callback.answer("❌ Tidak ada member non-admin untuk dihapus", show_alert=True)
        return
    
    await state.set_state(States.waiting_remove_member_id)
    await callback.message.answer(
        "➖ **HAPUS MEMBER**\n\n"
        "📝 Kirim User ID yang ingin dihapus dari member\n"
        "⚠️ Admin tidak bisa dihapus dari member list\n\n"
        "❌ Ketik /cancel untuk batal"
    )

@dp.callback_query(F.data == "list_members")
async def list_members_callback(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Akses ditolak", show_alert=True)
        return
    
    member_text = "👥 **DAFTAR MEMBER**\n\n"
    
    admin_list = []
    regular_members = []
    
    for member_id in MEMBER_IDS:
        if member_id in ADMIN_IDS:
            admin_list.append(member_id)
        else:
            regular_members.append(member_id)
    
    member_text += f"⚙️ **Admin ({len(admin_list)}):**\n"
    for admin_id in admin_list:
        try:
            user = await bot.get_chat(admin_id)
            name = user.first_name or "Unknown"
            member_text += f"   • {name} (`{admin_id}`)\n"
        except:
            member_text += f"   • ID: `{admin_id}` (Unknown)\n"
    
    member_text += f"\n👤 **Member Reguler ({len(regular_members)}):**\n"
    if regular_members:
        for member_id in regular_members:
            try:
                user = await bot.get_chat(member_id)
                name = user.first_name or "Unknown"
                member_text += f"   • {name} (`{member_id}`)\n"
            except:
                member_text += f"   • ID: `{member_id}` (Unknown)\n"
    else:
        member_text += "   (Tidak ada member reguler)\n"
    
    await callback.message.answer(member_text, parse_mode="Markdown")

@dp.message(StateFilter(States.waiting_member_id), F.text)
async def handle_add_member(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Akses ditolak!")
        await state.clear()
        return
    
    if message.text == "/cancel":
        await message.answer("❌ Penambahan member dibatalkan")
        await state.clear()
        return
    
    try:
        new_member_id = int(message.text.strip())
        
        if new_member_id in MEMBER_IDS:
            await message.answer(f"⚠️ User ID `{new_member_id}` sudah menjadi member!", parse_mode="Markdown")
            await state.clear()
            return
        
        try:
            user = await bot.get_chat(new_member_id)
            user_name = user.first_name or "Unknown User"
        except:
            user_name = "Unknown User"
        
        MEMBER_IDS.append(new_member_id)
        
        success_text = f"✅ **Member baru berhasil ditambahkan!**\n\n"
        success_text += f"👤 Nama: {user_name}\n"
        success_text += f"🆔 User ID: `{new_member_id}`\n"
        success_text += f"📊 Total Member: {len(MEMBER_IDS)}"
        
        await message.answer(success_text, parse_mode="Markdown")
        
        try:
            await bot.send_message(
                new_member_id,
                "🎉 **Selamat!**\n\n"
                "✅ Anda telah diupgrade menjadi **Member Premium**!\n\n"
                "🔓 Fitur yang sekarang bisa diakses:\n"
                "• 📄➡️📇 TXT to VCF Converter\n"
                "• Batch processing unlimited\n"
                "• Auto-format 30+ negara\n"
                "• Deduplication otomatis\n\n"
                "🚀 Gunakan /start untuk mulai menggunakan bot!",
                parse_mode="Markdown"
            )
        except:
            await message.answer("⚠️ Member ditambahkan tapi tidak bisa mengirim notifikasi ke user tersebut")
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Format User ID tidak valid! Harap kirim angka saja.\n\nContoh: 123456789")
    except Exception as e:
        await message.answer(f"❌ Error: {str(e)}")
        await state.clear()

@dp.message(StateFilter(States.waiting_remove_member_id), F.text)
async def handle_remove_member(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Akses ditolak!")
        await state.clear()
        return
    
    if message.text == "/cancel":
        await message.answer("❌ Penghapusan member dibatalkan")
        await state.clear()
        return
    
    try:
        remove_member_id = int(message.text.strip())
        
        if remove_member_id not in MEMBER_IDS:
            await message.answer(f"⚠️ User ID `{remove_member_id}` bukan member!", parse_mode="Markdown")
            await state.clear()
            return
        
        if remove_member_id in ADMIN_IDS:
            await message.answer("❌ Tidak bisa menghapus admin dari member list!")
            await state.clear()
            return
        
        try:
            user = await bot.get_chat(remove_member_id)
            user_name = user.first_name or "Unknown User"
        except:
            user_name = "Unknown User"
        
        MEMBER_IDS.remove(remove_member_id)
        
        success_text = f"✅ **Member berhasil dihapus!**\n\n"
        success_text += f"👤 Nama: {user_name}\n"
        success_text += f"🆔 User ID: `{remove_member_id}`\n"
        success_text += f"📊 Total Member: {len(MEMBER_IDS)}"
        
        await message.answer(success_text, parse_mode="Markdown")
        
        try:
            await bot.send_message(
                remove_member_id,
                "📢 **Pemberitahuan**\n\n"
                "❌ Status member premium Anda telah **dicabut**\n\n"
                "🔒 Fitur yang tidak bisa diakses:\n"
                "• TXT to VCF Converter\n\n"
                "💬 Hubungi admin jika ada pertanyaan",
                parse_mode="Markdown"
            )
        except:
            await message.answer("⚠️ Member dihapus tapi tidak bisa mengirim notifikasi ke user tersebut")
        
        if remove_member_id in user_cache:
            del user_cache[remove_member_id]
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Format User ID tidak valid! Harap kirim angka saja.\n\nContoh: 123456789")
    except Exception as e:
        await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
async def back_to_menu_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "🏠 **MENU UTAMA**\n\nPilih fitur yang ingin digunakan:",
        reply_markup=create_main_menu(user_id),
        parse_mode="Markdown"
    )

@dp.message(StateFilter(States.waiting_txt_files), F.document)
async def handle_txt_upload(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id not in user_cache:
        user_cache[user_id] = {"files": [], "type": "txt"}
    
    if message.document.mime_type == 'text/plain' or message.document.file_name.endswith('.txt'):
        file = await bot.get_file(message.document.file_id)
        file_content = await bot.download_file(file.file_path)
        user_cache[user_id]["files"].append(file_content.read())
        
        await message.answer(f"✅ File {message.document.file_name} berhasil diupload!\n📁 Total files: {len(user_cache[user_id]['files'])}\n\n⚠️ Ketik /done jika sudah selesai upload")
    else:
        await message.answer("❌ Hanya file .txt yang diizinkan!")

@dp.message(StateFilter(States.waiting_vcf_files), F.document)
async def handle_vcf_upload(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id not in user_cache:
        user_cache[user_id] = {"files": [], "type": "vcf"}
    
    if message.document.file_name.endswith('.vcf'):
        file = await bot.get_file(message.document.file_id)
        file_content = await bot.download_file(file.file_path)
        user_cache[user_id]["files"].append(file_content.read())
        
        await message.answer(f"✅ File {message.document.file_name} berhasil diupload!\n📁 Total files: {len(user_cache[user_id]['files'])}\n\n⚠️ Ketik /done jika sudah selesai upload")
    else:
        await message.answer("❌ Hanya file .vcf yang diizinkan!")

@dp.message(StateFilter(States.waiting_txt_files), F.text.in_(["/done", "done", "Done"]))
async def done_txt_upload(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id not in user_cache or not user_cache[user_id]["files"]:
        await message.answer("❌ Tidak ada file yang diupload!")
        return
    
    await state.set_state(States.waiting_batch_size)
    await message.answer("📊 **Masukkan jumlah kontak per file VCF** (default: 500):\n\n📝 Contoh: 1000")

@dp.message(StateFilter(States.waiting_vcf_files), F.text.in_(["/done", "done", "Done"]))
async def done_vcf_upload(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id not in user_cache or not user_cache[user_id]["files"]:
        await message.answer("❌ Tidak ada file yang diupload!")
        return
    
    await message.answer("🔄 **Memproses konversi VCF ke TXT...**")
    
    try:
        result_file = await process_vcf_to_txt(user_cache[user_id]["files"])
        
        await message.answer_document(
            FSInputFile(result_file, filename=result_file),
            caption="✅ **Konversi VCF ke TXT selesai!**\n\n📄 Format: Nama;Nomor"
        )
        
        os.remove(result_file)
        del user_cache[user_id]
        await state.clear()
        
    except Exception as e:
        await message.answer(f"❌ Error saat memproses: {str(e)}")
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🚨 **ERROR REPORT**\n\n"
                    f"👤 User ID: {user_id}\n"
                    f"🔧 Process: VCF to TXT\n"
                    f"❌ Error: {str(e)}"
                )
            except:
                pass

@dp.message(StateFilter(States.waiting_batch_size), F.text)
async def handle_batch_size(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        batch_size = int(message.text) if message.text.isdigit() else 500
        batch_size = max(1, min(batch_size, 5000))
        user_cache[user_id]["batch_size"] = batch_size
        await state.set_state(States.waiting_contact_prefix)
        await message.answer("📝 Masukkan prefix nama kontak (contoh: Pelanggan-)")
    except ValueError:
        await message.answer("❌ Masukkan angka yang valid! (contoh: 500)")
        
        if not processing_queue.empty():
            next_task = await processing_queue.get()
            asyncio.create_task(process_queued_task(next_task))
        
    except ValueError:
        await message.answer("❌ Masukkan angka yang valid! (contoh: 500)")
    except Exception as e:
        await message.answer(f"❌ Error saat memproses: {str(e)}")
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🚨 **ERROR REPORT**\n\n"
                    f"👤 User ID: {user_id}\n"
                    f"📊 Valid: {info.get('stats', {}).get('valid', 0)}\n"
                    f"🚫 Invalid: {info.get('stats', {}).get('invalid', 0)}\n"
                    f"❌ Error: {str(e)}"
                )
            except:
                pass
        
        current_processing = None
        if user_id in user_cache:
            del user_cache[user_id]
        await state.clear()

async def process_queued_task(task_data):
    global current_processing
    
    user_id = task_data['user_id']
    files = task_data['files']
    batch_size = task_data['batch_size']
    message = task_data['message']
    
    current_processing = user_id
    
    try:
        await message.answer("🔄 **Memulai proses konversi...**")
        
        result_files, info = await process_txt_to_vcf(files, batch_size, user_id)
        
        elapsed = info['elapsed']
        if elapsed < 2:
            speed_emoji = "⚡"
        elif elapsed < 10:
            speed_emoji = "🚀"
        else:
            speed_emoji = "🕒"
        
        for file_path in result_files:
            await message.answer_document(
                FSInputFile(file_path, filename=os.path.basename(file_path))
            )
            os.remove(file_path)
        
        summary_text = f"✅ **Konversi TXT ke VCF selesai!** {speed_emoji}\n\n"
        summary_text += f"📁 Total file VCF: {len(result_files)}\n"
        summary_text += f"⏱ Waktu proses: {elapsed:.2f}s\n"
        summary_text += f"📊 Total kontak: {info['stats']['valid']}"
        
        await message.answer(summary_text)
        
    except Exception as e:
        await message.answer(f"❌ Error saat memproses: {str(e)}")
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🚨 **ERROR REPORT**\n\n"
                    f"👤 User ID: {user_id}\n"
                    f"❌ Error: {str(e)}"
                )
            except:
                pass
    
    finally:
        current_processing = None
        if not processing_queue.empty():
            next_task = await processing_queue.get()
            asyncio.create_task(process_queued_task(next_task))

@dp.message(Command("status_vps"))
async def status_vps_command(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Akses ditolak!")
        return
    
    try:
        ping_time = ping3.ping('8.8.8.8') or 0
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        boot_time = psutil.boot_time()
        uptime = datetime.now() - datetime.fromtimestamp(boot_time)
        
        status_text = f"💻 **STATUS VPS**\n\n"
        status_text += f"📡 Ping: {ping_time*1000:.0f}ms\n"
        status_text += f"⚙️ CPU: {cpu_percent:.1f}%\n"
        status_text += f"💾 RAM: {memory.percent:.1f}% ({memory.used//1024//1024}MB/{memory.total//1024//1024}MB)\n"
        status_text += f"💿 Storage: {disk.percent:.1f}% ({disk.used//1024//1024//1024}GB/{disk.total//1024//1024//1024}GB)\n"
        status_text += f"⏱ Uptime: {uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m\n"
        status_text += f"👥 Active users: {len(user_cache)}\n"
        status_text += f"⏳ Queue: {processing_queue.qsize()}\n"
        
        await message.answer(status_text, parse_mode="Markdown")
        
    except Exception as e:
        await message.answer(f"❌ Error getting VPS status: {str(e)}")

@dp.message(Command("hapus_cache"))
async def hapus_cache_command(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Akses ditolak!")
        return
    
    user_id = message.from_user.id
    if user_id in user_cache:
        del user_cache[user_id]
    
    await message.answer("✅ Cache user berhasil dihapus")

@dp.message(Command("hapus_semua_cache"))
async def hapus_semua_cache_command(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Akses ditolak!")
        return
    
    cache_count = len(user_cache)
    user_cache.clear()
    await message.answer(f"✅ Semua cache berhasil dihapus ({cache_count} users)")

@dp.message(F.text)
async def handle_unknown_message(message: Message):
    await message.answer(
        "❓ Perintah tidak dikenali\n\n"
        "Gunakan /start untuk menu utama atau /help untuk bantuan",
        reply_markup=create_main_menu(message.from_user.id)
    )

async def main():
    print("🤖 Bot VCF Converter starting...")
    print(f"📊 Loaded {len(kode_negara)} country codes")
    print(f"👥 {len(MEMBER_IDS)} members configured")
    print(f"⚙️ {len(ADMIN_IDS)} admins configured")
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        print(f"❌ Bot error: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())

@dp.message(StateFilter(States.waiting_contact_prefix), F.text)
async def handle_contact_prefix(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user_cache[user_id]["contact_prefix"] = message.text.strip()
    await state.set_state(States.waiting_output_filename)
    await message.answer("💾 Masukkan nama file output tanpa ekstensi (contoh: kontak_januari)")

@dp.message(StateFilter(States.waiting_output_filename), F.text)
async def handle_output_filename(message: Message, state: FSMContext):
    global current_processing
    user_id = message.from_user.id
    output_filename = message.text.strip().replace(" ", "_")
    batch_size = user_cache[user_id]["batch_size"]
    contact_prefix = user_cache[user_id]["contact_prefix"]
    if current_processing:
        await processing_queue.put({
            'user_id': user_id,
            'files': user_cache[user_id]["files"],
            'batch_size': batch_size,
            'contact_prefix': contact_prefix,
            'output_filename': output_filename,
            'message': message
        })
        position = processing_queue.qsize()
        await message.answer(f"⏳ Posisi antrian: #{position}\n⏱ Estimasi tunggu: {position * 2} menit")
        return
    current_processing = user_id
    await message.answer("📊 **Menganalisis file...**")
    result_files, info = await process_txt_to_vcf(user_cache[user_id]["files"], batch_size, user_id, contact_prefix, output_filename)
    stats_text = f"📂 Jumlah file .txt terbaca: {info['total_files']}\n"
    stats_text += f"📊 Jumlah kontak valid total: {info['stats']['valid']}\n"
    stats_text += f"🚫 Jumlah kontak invalid: {info['stats']['invalid']}\n"
    stats_text += f"📌 Rekap lengkap per negara:\n"
    for country, count in sorted(info['stats']['countries'].items(), key=lambda x: x[1], reverse=True):
        stats_text += f"   {country}: {count} kontak\n"
    await message.answer(stats_text)
    elapsed = info['elapsed']
    if elapsed < 2:
        speed_emoji = "⚡"
    elif elapsed < 10:
        speed_emoji = "🚀"
    else:
        speed_emoji = "🕒"
    for file_path in result_files:
        await message.answer_document(
            FSInputFile(file_path, filename=os.path.basename(file_path))
        )
        os.remove(file_path)
    summary_text = f"✅ **Konversi TXT ke VCF selesai!** {speed_emoji}\n\n"
    summary_text += f"📁 Total file VCF: {len(result_files)}\n"
    summary_text += f"⏱ Waktu proses: {elapsed:.2f}s\n"
    summary_text += f"📊 Total kontak: {info['stats']['valid']}"
    await message.answer(summary_text)
    del user_cache[user_id]
    await state.clear()
    current_processing = None
    if not processing_queue.empty():
        next_task = await processing_queue.get()
        asyncio.create_task(process_queued_task(next_task))
