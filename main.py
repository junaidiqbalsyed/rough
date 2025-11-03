import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

# -----------------------------
# Config
# -----------------------------
SEED = 42                    # set to None for non-deterministic runs
N_ROWS = 100                 # change as needed
OUTFILE = "fake_call_extractions.xlsx"

fake = Faker()
if SEED is not None:
    random.seed(SEED)
    Faker.seed(SEED)

# -----------------------------
# Literals (from your main.py)
# -----------------------------
CALL_TYPE = [
    "Inbound", "Outbound", "Follow-up", "Escalation", "Callback", "Voicemail"
]
CALL_OUTCOME = ["Resolved", "Escalated", "Dropped", "Transferred"]
EMOTION = ["Neutral", "Confused", "Frustrated", "Angry",
           "Anxious", "Distressed", "Relieved", "Grateful"]
CALL_REASON = [
    "Eligibility or Coverage Inquiry",
    "Benefits Access or Card Issues",
    "Claims or Payments",
    "Prior Authorization or Referrals",
    "Provider Enrollment or Credentialing",
    "Member Information Update",
    "Program Education or Guidance",
    "Technical Support or Portal Issues",
    "Complaint or Grievance",
    "General Inquiry or Other",
    "Pharmacy or Prescription Issue",
    "Service Authorization Status",
    "Appeal or Denial Follow-up",
    "Appointment Scheduling or Transportation",
    "Document Submission or Verification",
]

# A small word pool to create short, realistic phrases/quotes
WORD_POOL = [
    "benefits", "card", "stopped", "not", "working", "provider", "enrollment", "update",
    "email", "address", "portal", "issue", "claim", "payment", "denial", "appeal",
    "authorization", "referral", "coverage", "eligibility", "revalidation", "status",
    "Medicaid", "contact", "transportation", "appointment", "document", "submission"
]

def short_phrase(min_w=5, max_w=9):
    n = random.randint(min_w, max_w)
    words = random.sample(WORD_POOL, k=min(n, len(WORD_POOL)))
    # Capitalize first word and add period at the end (for quotes)
    return (" ".join(words)).capitalize()

def question_summary():
    # 5-7 words, no punctuation
    n = random.randint(5, 7)
    words = random.sample(WORD_POOL, k=min(n, len(WORD_POOL)))
    return " ".join(words)

def make_questions():
    # 1–3 unique questions; store as list[ {question, quote} ]
    k = random.randint(1, 3)
    items = []
    for _ in range(k):
        q = question_summary()
        quote = short_phrase(6, 10)
        items.append({"question": q, "quote": quote})
    return items

def make_themes():
    # 1–3 themes; each has theme (plain phrase), emotion (enum), quote (<=25 words)
    k = random.randint(1, 3)
    items = []
    used = set()
    while len(items) < k:
        theme = short_phrase(2, 5)
        if theme not in used:
            used.add(theme)
            items.append({
                "theme": theme,
                "emotion": random.choice(EMOTION),
                "quote": short_phrase(6, 12)
            })
    return items

def random_timestamp():
    # Within the past 180 days
    days_back = random.randint(0, 180)
    dt = datetime.utcnow() - timedelta(days=days_back, hours=random.randint(0, 23),
                                       minutes=random.randint(0, 59))
    return dt.replace(microsecond=0).isoformat() + "Z"

def random_filename(callid):
    return f"{fake.file_name(category='video')}".replace(" ", "_").replace("/", "_")

def random_agent_name():
    # Avoid last_name_initial() (often missing). Build ourselves.
    return f"{fake.first_name()} {fake.last_name()[0]}."

def make_row(i: int) -> dict:
    callid = fake.unique.random_number(digits=12, fix_len=True)
    total_call_time = round(random.uniform(0.5, 15.0), 2)  # minutes

    call_type = random.choice(CALL_TYPE)
    call_category = random.choice(CALL_REASON)
    call_outcome = random.choice(CALL_OUTCOME)

    # 10–15 word primary reason (caller perspective)
    primary_reason = " ".join(fake.words(nb=random.randint(10, 15))).capitalize()

    questions = make_questions()
    themes = make_themes()

    row = {
        # passthrough / metadata
        "callid": str(callid),
        "filename": random_filename(callid),
        "timestamp": random_timestamp(),
        "agent": random_agent_name(),
        "account_id": fake.bothify(text="ACC-####-??"),
        "total_call_time": total_call_time,

        # extraction
        "primary_reason": primary_reason,
        "call_type": call_type,
        "call_category": call_category,
        "call_outcome": call_outcome,
        "questions": questions,          # will be JSON-serialized before export
        "themes": themes,                # will be JSON-serialized before export
        "sentiment_score": random.randint(0, 10),   # 0=most negative, 10=most positive
        "food_program": random.choice([True, False]),
    }
    return row

def generate_dataframe(n_rows: int) -> pd.DataFrame:
    rows = [make_row(i) for i in range(n_rows)]
    df = pd.DataFrame(rows)
    # Serialize list-of-dicts columns to JSON strings for a single-sheet Excel
    df["questions"] = df["questions"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    df["themes"] = df["themes"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    return df

def main(n_rows=N_ROWS, outfile=OUTFILE):
    df = generate_dataframe(n_rows)
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(outfile, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="calls")
    print(f"Saved {len(df)} rows to {outfile}")

if __name__ == "__main__":
    main()