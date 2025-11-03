import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

# ======== CONFIG ========
N_ROWS = 100
OUTFILE = "fake_call_extractions_faker.xlsx"

# Enumerations
CALL_TYPE = ["Inbound", "Outbound", "Follow-up", "Escalation", "Callback", "Voicemail"]
CALL_OUTCOME = ["Resolved", "Escalated", "Dropped", "Transferred"]
EMOTION = ["Neutral", "Confused", "Frustrated", "Angry", "Anxious", "Distressed", "Relieved", "Grateful"]
CALL_REASON = [
    "Eligibility or Coverage Inquiry", "Benefits Access or Card Issues",
    "Claims or Payments", "Prior Authorization or Referrals",
    "Provider Enrollment or Credentialing", "Member Information Update",
    "Program Education or Guidance", "Technical Support or Portal Issues",
    "Complaint or Grievance", "General Inquiry or Other",
    "Pharmacy or Prescription Issue", "Service Authorization Status",
    "Appeal or Denial Follow-up", "Appointment Scheduling or Transportation",
    "Document Submission or Verification",
]

WORD_POOL = [
    "benefits","card","stopped","not","working","provider","enrollment","update",
    "email","address","portal","issue","claim","payment","denial","appeal",
    "authorization","referral","coverage","eligibility","revalidation","status",
    "Medicaid","contact","transportation","appointment","document","submission"
]

fake = Faker()
random.seed(42)
Faker.seed(42)

# ======== HELPERS ========
def short_phrase(min_w=5, max_w=9):
    n = random.randint(min_w, max_w)
    words = random.sample(WORD_POOL, k=min(n, len(WORD_POOL)))
    return (" ".join(words)).capitalize()

def generate_questions():
    k = random.randint(1, 3)
    items = []
    for _ in range(k):
        question = " ".join(random.sample(WORD_POOL, k=random.randint(5,7)))
        quote = short_phrase(6, 10)
        items.append({"question": question, "quote": quote})
    return items

def generate_themes():
    k = random.randint(1, 3)
    items = []
    used = set()
    for _ in range(k):
        theme = " ".join(random.sample(WORD_POOL, k=random.randint(2,5))).capitalize()
        if theme in used:
            continue
        used.add(theme)
        emotion = random.choice(EMOTION)
        quote = short_phrase(6, 12)
        items.append({"theme": theme, "emotion": emotion, "quote": quote})
    return items

def random_timestamp():
    delta = timedelta(days=random.randint(0, 365), hours=random.randint(0, 23),
                      minutes=random.randint(0, 59))
    dt = datetime.utcnow() - delta
    return dt.replace(microsecond=0).isoformat() + "Z"

def make_row():
    callid = str(fake.unique.random_number(digits=12, fix_len=True))
    primary = short_phrase(10, 15)
    questions = generate_questions()
    themes = generate_themes()
    row = {
        "callid": callid,
        "filename": f"{callid}.mp4",
        "timestamp": random_timestamp(),
        "agent": f"{fake.first_name()} {fake.last_name()[0]}.",
        "account_id": fake.bothify("ACC-####-??"),
        "total_call_time": round(random.uniform(0.5, 15.0), 2),

        "primary_reason": primary,
        "call_type": random.choice(CALL_TYPE),
        "call_category": random.choice(CALL_REASON),
        "call_reason": None,  # alias
        "call_outcome": random.choice(CALL_OUTCOME),
        "emotion": random.choice(EMOTION),
        "questions": json.dumps(questions, ensure_ascii=False),
        "themes": json.dumps(themes, ensure_ascii=False),
        "sentiment_score": random.randint(0, 10),
        "food_program": random.choice([True, False]),
    }
    row["call_reason"] = row["call_category"]
    return row

def create_dataset(n_rows: int):
    rows = [make_row() for _ in range(n_rows)]
    return pd.DataFrame(rows)

def main():
    df = create_dataset(N_ROWS)
    Path(OUTFILE).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTFILE, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="calls")
    print(f"Saved {len(df)} rows to {OUTFILE}")

if __name__ == "__main__":
    main()