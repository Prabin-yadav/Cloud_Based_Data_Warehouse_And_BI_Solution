"""
config.py  —  Zentrik Pharma ETL Configuration
Date shift: AW data (2010-2014) → shifted to (2022-2026)
"""

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


_ROOT = Path(__file__).resolve().parents[1]
if load_dotenv is not None:
    load_dotenv(_ROOT / ".env", override=False)
    load_dotenv(override=False)


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    v = _env(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

# ── Source: SQL Server (AdventureWorks) ──────────────────────────
# Defaults are intentionally generic; set AW_SOURCE_* in .env for your machine.
SOURCE_SERVER   = _env("AW_SOURCE_SERVER", r".\SQLEXPRESS")
SOURCE_DATABASE = _env("AW_SOURCE_DATABASE", "AdventureWorksDW2025")
SOURCE_CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SOURCE_SERVER};"
    f"DATABASE={SOURCE_DATABASE};"
    f"Trusted_Connection=yes;"
)

# ── Target: AWS RDS PostgreSQL ────────────────────────────────────
# Prefer RDS_* vars (also used by the Streamlit app).
TARGET_HOST     = _env("RDS_HOST", "")
TARGET_PORT     = _env_int("RDS_PORT", 5432)
TARGET_DBNAME   = _env("RDS_DBNAME", "")
TARGET_USER     = _env("RDS_USER", "")
TARGET_PASSWORD = _env("RDS_PASSWORD", "")
TARGET_SSLMODE  = _env("RDS_SSLMODE", "require")

# ── Date Shift ────────────────────────────────────────────────────
# Shifts all dates forward by 11 years so data appears as 2021-2025
# instead of original 2010-2014 from AdventureWorks
DATE_SHIFT_YEARS = 12

# dim_date will be generated from 2020-01-01 to 2030-12-31
DIM_DATE_START  = "2021-01-01"
DIM_DATE_END    = "2030-12-31"

# ── Batch settings ────────────────────────────────────────────────
BATCH_SIZE      = 5000
ETL_BATCH_ID    = "AW_FULL_LOAD_DATE_SHIFTED"

# ── Pharma Mappings ───────────────────────────────────────────────
CATEGORY_TO_THERAPEUTIC_CLASS = {
    "Bikes":        ("Cardiovascular Drugs",    "C01", "ATC-C"),
    "Components":   ("Antibiotics",             "J01", "ATC-J"),
    "Clothing":     ("Dermatological Agents",   "D01", "ATC-D"),
    "Accessories":  ("Analgesics",              "N02", "ATC-N"),
}

SUBCATEGORY_TO_SUBCLASS = {
    "Mountain Bikes":    "Beta Blockers",
    "Road Bikes":        "ACE Inhibitors",
    "Touring Bikes":     "Calcium Channel Blockers",
    "Handlebars":        "Penicillins",
    "Bottom Brackets":   "Cephalosporins",
    "Brakes":            "Macrolides",
    "Jerseys":           "Topical Steroids",
    "Shorts":            "Antifungals",
    "Vests":             "Emollients",
    "Helmets":           "NSAIDs",
    "Hydration Packs":   "Opioids",
    "Tires and Tubes":   "Quinolones",
    "Pedals":            "Tetracyclines",
    "Saddles":           "Aminoglycosides",
    "Wheels":            "Carbapenems",
    "Gloves":            "Antivirals",
    "Socks":             "Antifungal Topical",
    "Caps":              "Antiparasitic",
    "Fenders":           "Analgesic Topical",
    "Locks":             "Muscle Relaxants",
    "Panniers":          "Antihistamines",
    "Pumps":             "Antacids",
    "Lights":            "Decongestants",
}

# Replace the existing DRUG_NAMES dict with this expanded version
DRUG_NAMES = {
    "Mountain Bikes":    [
        "Ramipril","Lisinopril","Enalapril","Captopril","Perindopril",
        "Trandolapril","Quinapril","Fosinopril","Benazepril","Moexipril"
    ],
    "Road Bikes":        [
        "Metoprolol","Atenolol","Bisoprolol","Carvedilol","Propranolol",
        "Nebivolol","Acebutolol","Betaxolol","Labetalol","Nadolol"
    ],
    "Touring Bikes":     [
        "Amlodipine","Nifedipine","Diltiazem","Verapamil","Felodipine",
        "Isradipine","Nimodipine","Nisoldipine","Clevidipine","Lercanidipine"
    ],
    "Handlebars":        [
        "Amoxicillin","Ampicillin","Cloxacillin","Flucloxacillin","Dicloxacillin",
        "Nafcillin","Oxacillin","Piperacillin","Ticarcillin","Carbenicillin"
    ],
    "Bottom Brackets":   [
        "Azithromycin","Erythromycin","Clarithromycin","Roxithromycin","Telithromycin",
        "Spiramycin","Josamycin","Midecamycin","Troleandomycin","Fidaxomicin"
    ],
    "Brakes":            [
        "Ciprofloxacin","Levofloxacin","Ofloxacin","Norfloxacin","Moxifloxacin",
        "Gatifloxacin","Sparfloxacin","Enoxacin","Pefloxacin","Fleroxacin"
    ],
    "Jerseys":           [
        "Betamethasone","Clobetasol","Hydrocortisone","Mometasone","Triamcinolone",
        "Fluticasone","Prednisolone","Dexamethasone","Budesonide","Beclomethasone"
    ],
    "Shorts":            [
        "Clotrimazole","Fluconazole","Miconazole","Ketoconazole","Itraconazole",
        "Voriconazole","Posaconazole","Terbinafine","Griseofulvin","Nystatin"
    ],
    "Vests":             [
        "Emollient Cream","Aqueous Cream","Paraffin Cream","Urea Cream","Calamine Cream",
        "Zinc Cream","Lanolin Cream","Cetomacrogol Cream","Oat Cream","Aloe Vera Cream"
    ],
    "Helmets":           [
        "Ibuprofen","Naproxen","Diclofenac","Aspirin","Celecoxib",
        "Etoricoxib","Indomethacin","Meloxicam","Piroxicam","Ketoprofen"
    ],
    "Hydration Packs":   [
        "Tramadol","Codeine","Morphine","Oxycodone","Fentanyl",
        "Buprenorphine","Hydrocodone","Tapentadol","Methadone","Pethidine"
    ],
    "Tires and Tubes":   [
        "Tetracycline","Doxycycline","Minocycline","Oxytetracycline","Chlortetracycline",
        "Demeclocycline","Meclocycline","Methacycline","Rolitetracycline","Lymecycline"
    ],
    "Pedals":            [
        "Gentamicin","Streptomycin","Neomycin","Tobramycin","Amikacin",
        "Kanamycin","Netilmicin","Sisomicin","Isepamicin","Plazomicin"
    ],
    "Saddles":           [
        "Losartan","Valsartan","Irbesartan","Candesartan","Olmesartan",
        "Telmisartan","Azilsartan","Eprosartan","Fimasartan","Saprisartan"
    ],
    "Wheels":            [
        "Meropenem","Imipenem","Ertapenem","Doripenem","Biapenem",
        "Faropenem","Panipenem","Razupenem","Tebipenem","Sulopenem"
    ],
    "Gloves":            [
        "Acyclovir","Valacyclovir","Famciclovir","Ganciclovir","Valganciclovir",
        "Cidofovir","Foscarnet","Penciclovir","Docosanol","Idoxuridine"
    ],
    "Socks":             [
        "Terbinafine","Griseofulvin","Itraconazole","Voriconazole","Posaconazole",
        "Amorolfine","Ciclopirox","Butenafine","Naftifine","Tolnaftate"
    ],
    "Caps":              [
        "Mebendazole","Albendazole","Ivermectin","Praziquantel","Pyrantel",
        "Levamisole","Niclosamide","Diethylcarbamazine","Triclabendazole","Oxamniquine"
    ],
    "Fenders":           [
        "Diclofenac Gel","Ibuprofen Gel","Piroxicam Gel","Ketoprofen Gel","Indomethacin Gel",
        "Nimesulide Gel","Naproxen Gel","Flurbiprofen Gel","Aceclofenac Gel","Lornoxicam Gel"
    ],
    "Locks":             [
        "Baclofen","Tizanidine","Cyclobenzaprine","Methocarbamol","Carisoprodol",
        "Chlorzoxazone","Orphenadrine","Dantrolene","Quinine","Tolperisone"
    ],
    "Panniers":          [
        "Cetirizine","Loratadine","Fexofenadine","Desloratadine","Levocetirizine",
        "Bilastine","Rupatadine","Ebastine","Azelastine","Mizolastine"
    ],
    "Pumps":             [
        "Omeprazole","Pantoprazole","Esomeprazole","Lansoprazole","Rabeprazole",
        "Dexlansoprazole","Ilaprazole","Tenatoprazole","Vonoprazan","Revaprazan"
    ],
    "Lights":            [
        "Pseudoephedrine","Phenylephrine","Oxymetazoline","Xylometazoline","Naphazoline",
        "Tramazoline","Tuaminoheptane","Synephrine","Octodrine","Propylhexedrine"
    ],
    "General":           [
        "Paracetamol","Chlorpheniramine","Dextromethorphan","Guaifenesin","Phenylephrine",
        "Caffeine","Vitamin C","Zinc Sulphate","Magnesium","Calcium Carbonate"
    ],
}

COLOR_TO_DOSAGE_FORM = {
    "Blue":   "Capsule",
    "Red":    "Tablet",
    "Black":  "Injection",
    "White":  "Syrup",
    "Yellow": "Cream",
    "Silver": "Ointment",
    "Grey":   "Gel",
    "Multi":  "Suspension",
}

SIZE_TO_STRENGTH = {
    "S":  "100mg",
    "M":  "250mg",
    "L":  "500mg",
    "XL": "1000mg",
    "38": "200mg",
    "40": "400mg",
    "42": "600mg",
    "44": "800mg",
    "48": "1200mg",
    "52": "1500mg",
    "56": "2000mg",
    "58": "50mg",
    "60": "150mg",
    "62": "300mg",
}

CUSTOMER_KEY_TO_TYPE = {
    (1,    5000):  ("Hospital",              "Government"),
    (5001, 10000): ("Pharmacy",              "Retail"),
    (10001,15000): ("Clinic",                "Private"),
    (15001,20000): ("Wholesale Distributor", "Wholesale"),
    (20001,99999): ("Retail Chain",          "Retail"),
}

REGULATORY_CATEGORY = {
    "Cardiovascular Drugs":  "Schedule H",
    "Antibiotics":           "Schedule H1",
    "Dermatological Agents": "Schedule G",
    "Analgesics":            "Schedule H",
}