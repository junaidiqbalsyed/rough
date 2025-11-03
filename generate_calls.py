#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate synthetic call-center data (SNAP/WIC) to your schema using Faker.
Outputs:
  - CSVs: calls.csv, themes.csv, tags.csv, questions.csv, urgency_quotes.csv, utterances.csv
  - Excel: calls_dataset.xlsx (one sheet per entity)
  - JSONL: calls.jsonl (full nested objects per line)
"""

import json
import random
import re
from datetime import datetime, timedelta, timezone

import pandas as pd
from dateutil.relativedelta import relativedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# ----------------------- Enumerations / lookups -----------------------

PROGRAMS = [
    ("SNAP", "Food Assistance"),
    ("WIC", "Women, Infants, and Children"),
]

CALL_TYPES = ["Inbound", "Outbound", "Follow-up", "Escalation", "Callback", "Voicemail"]
CALL_OUTCOMES = ["Resolved", "Escalated", "Dropped", "Transferred"]
CALL_REASONS = [
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

PRIMARY_INTENTS = [
    "Product Support",
    "Sales Inquiry",
    "Billing / Payments",
    "Retention / Cancellation",
    "Marketing Campaign",
]

SENTIMENT_LABELS = [
    "Neutral", "Confused", "Frustrated", "Angry",
    "Anxious", "Distressed", "Relieved", "Grateful"
]

# ISO 639-1 language codes to sample
LANGS = ["en", "es"]

US_REGIONS = {
    "Northeast": ["ME","NH","VT","MA","RI","CT","NY","NJ","PA"],
    "Midwest":   ["OH","MI","IN","IL","WI","MN","IA","MO","ND","SD","NE","KS"],
    "South":     ["DE","MD","DC","VA","WV","NC","SC","GA","FL","KY","TN","MS","AL","OK","TX","AR","LA"],
    "West":      ["ID","MT","WY","NV","UT","CO","AZ","NM","AK","WA","OR","CA","HI"],
}

# ----------------------- Small utilities -----------------------

def pick_state_and_region():
    region = random.choice(list(US_REGIONS.keys()))
    state = random.choice(US_REGIONS[region])
    return state, region

def iso8601(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_week_yyyynn(dt: datetime) -> int:
    year, week, _ = dt.isocalendar()
    return int(f"{year}{week:02d}")

def make_call_id(program_name: str, idx: int, dt: datetime) -> str:
    return f"{program_name}-{dt.year}-{idx:05d}"

def maybe_none(p=0.15):
    """Return None with probability p; else True (just to trigger non-None)."""
    return None if random.random() < p else True

def random_sentiment():
    label = random.choice(SENTIMENT_LABELS)
    score = round(random.uniform(0, 1), 2)  # 0..1 scale
    return label, score

def random_snippets(program_name: str):
    if program_name == "SNAP":
        return [
            "Hi, I need help with my SNAP card.",
            "Hello, I applied for SNAP but have a question.",
            "Hi, I think there's an issue with my EBT card.",
        ]
    return [
        "Hi, I missed my WIC appointment.",
        "Hello, I need to reschedule my WIC visit.",
        "Hi, I have a question about WIC benefits.",
    ]

def contains_question(text: str) -> bool:
    return "?" in text

def last_initial_from_last_name() -> str:
    """Build a robust last-name initial like 'S.' even if Faker returns hyphenated names."""
    ln = fake.last_name()
    m = re.search(r"[A-Za-z]", ln)
    return (m.group(0).upper() + ".") if m else "X."

# ----------------------- Builders for arrays -----------------------

def make_themes():
    CANNED = [
        "Eligibility",
        "Benefits Amount",
        "Card Activation and Delivery",
        "Program Enrollment Process",
        "Appointment Scheduling and Reminders",
        "Transportation Issues",
        "Portal Access",
        "Document Verification",
    ]
    n = random.randint(1, 3)
    out = []
    for _ in range(n):
        out.append({
            "theme_name": random.choice(CANNED),
            "theme_level": random.choice([1, 2]),
            "theme_confidence": round(random.uniform(0.6, 0.98), 2),
        })
    # de-duplicate by name
    seen = set()
    uniq = []
    for t in out:
        if t["theme_name"] not in seen:
            seen.add(t["theme_name"])
            uniq.append(t)
    return uniq

def make_tags():
    candidates = [
        "October Benefit Cycle", "EBT Support", "Reschedule Support",
        "Missed Appointment", "Portal Login", "Fall Campaign", "Verification"
    ]
    k = random.randint(0, 3)
    return random.sample(candidates, k=k)

def make_questions():
    candidates = [
        "When will I receive my new SNAP card?",
        "Can I reschedule my WIC appointment if I miss it?",
        "Is there transportation available?",
        "Why was my claim denied?",
        "How do I submit documents online?",
        "How can I update my address?",
    ]
    n = random.randint(0, 2)
    return [{"question_text": random.choice(candidates), "occurrences_count": random.randint(1, 2)}
            for _ in range(n)]

def make_urgency_quotes(duration_sec: int):
    samples = [
        "I’ve been waiting for two weeks and still haven’t got the card.",
        "My appointment is tomorrow, I need to confirm today.",
        "I can’t access food benefits this week, please help.",
        "I missed the appointment, I don’t want to lose benefits.",
    ]
    n = random.randint(0, 2)
    out = []
    for _ in range(n):
        out.append({
            "quote_text": random.choice(samples),
            "quote_offset_sec": random.randint(0, max(5, duration_sec - 5)),
            "score": round(random.uniform(0.4, 0.95), 2),
        })
    return out

def simulate_utterances(duration_sec: int, program_name: str):
    turns = []
    t = 0
    ms_total = duration_sec * 1000
    say = random_snippets(program_name)
    opener = random.choice(say)

    pieces = [
        {"speaker": "caller", "text": opener},
        {"speaker": "agent", "text": "I can help with that. Could you confirm your case ID or name on the account?"},
        {"speaker": "caller", "text": "Yes, it’s under " + fake.name() + "."},
        {"speaker": "agent", "text": "Thanks. I see your record—one moment while I check the status."},
        {"speaker": "agent", "text": "Good news, it’s resolved. You should receive confirmation shortly."},
        {"speaker": "caller", "text": "Great, thanks a lot!"},
    ]

    avg_turn_ms = max(3000, ms_total // (len(pieces) + random.randint(0, 2)))
    for p in pieces:
        start_ms = t
        end_ms = min(ms_total, t + avg_turn_ms + random.randint(-800, 800))
        t = end_ms + random.randint(200, 600)
        label, score = random_sentiment()
        turns.append({
            "speaker": p["speaker"],
            "start_ms": max(0, start_ms),
            "end_ms": max(0, end_ms),
            "text": p["text"],
            "utterance_sentiment_label": label,
            "utterance_sentiment_score": score,
            "toxicity_score": round(random.uniform(0.0, 0.15), 2),
            "contains_question": contains_question(p["text"]),
        })
        if t >= ms_total:
            break
    return turns

# ----------------------- Main record builder -----------------------

def build_record(idx: int, start_anchor: datetime):
    program_name, program_type = random.choice(PROGRAMS)
    state_code, region = pick_state_and_region()

    # times within ~60 days window ending now (UTC)
    start_dt = start_anchor - relativedelta(days=random.randint(0, 60),
                                            hours=random.randint(0, 10),
                                            minutes=random.randint(0, 59))
    duration_sec = random.randint(120, 900)
    end_dt = start_dt + timedelta(seconds=duration_sec)

    call_id = make_call_id(program_name, idx, start_dt)
    transcript_id = f"TX-{random.randint(100000, 999999)}" if maybe_none() else None

    s_label, s_score = random_sentiment()
    urgency_score = round(random.uniform(0.0, 0.95), 2)

    # Safer "First L." format (no last_name_initial() in some Faker versions)
    agent_name = f"{fake.first_name()} {last_initial_from_last_name()}"

    account_name = (
        f"{state_code} {program_name} Support Center"
        if program_name == "SNAP"
        else f"{fake.city()} {program_name} Helpline"
    )

    intent = {
        "primary_intent": random.choice(PRIMARY_INTENTS),
        "sub_intent": random.choice([
            "Card not received", "Appointment reschedule", "Address update",
            "Portal reset", "Claim status", "Document upload issue"
        ])
    }

    themes = make_themes()
    tags = make_tags()
    questions = make_questions()
    urgency_quotes = make_urgency_quotes(duration_sec)
    utterances = simulate_utterances(duration_sec, program_name)

    start_week = iso_week_yyyynn(start_dt)
    start_dow = start_dt.isoweekday()  # 1..7

    record = {
        "call_id": call_id,
        "transcript_id": transcript_id,

        "program": {
            "program_name": program_name,
            "program_type": program_type
        },
        "account": {
            "account_name": account_name,
            "business_line": "Public Assistance" if program_name == "SNAP" else "Maternal & Child Health"
        },
        "state": {
            "state_code": state_code,
            "region": region
        },

        "channel": "voice",

        "call_type": random.choice(CALL_TYPES),
        "call_outcome": random.choice(CALL_OUTCOMES),
        "call_reason": random.choice(CALL_REASONS),

        "start_time_utc": iso8601(start_dt),
        "end_time_utc": iso8601(end_dt),
        "duration_sec": duration_sec,
        "language": random.choice(LANGS),
        "consent_captured": bool(random.getrandbits(1)),

        "agent": {
            "agent_name": agent_name
        },

        "intent": intent,

        "disposition": random.choice(["Resolved", "Escalated", "Dropped", "Transferred", "Voicemail"]),

        "sentiment": {
            "sentiment_label": s_label,
            "sentiment_score": s_score
        },

        "urgency_score": urgency_score,

        "themes": themes,
        "tags": tags,
        "questions": questions,
        "urgency_quotes": urgency_quotes,
        "utterances": utterances,

        "time_features": {
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "start_hour": start_dt.hour,
            "start_dow": start_dow,
            "start_week": start_week,
            "start_month": start_dt.strftime("%Y-%m")
        }
    }
    return record

# ----------------------- Dataset generator -----------------------

def generate_dataset(n_rows=200, seed=42):
    Faker.seed(seed)
    random.seed(seed)
    now = datetime.now(timezone.utc)

    nested_records = []

    themes_rows, tags_rows, questions_rows = [], [], []
    urgency_rows, utter_rows = [], []
    main_rows = []

    for i in range(1, n_rows + 1):
        rec = build_record(i, now)
        nested_records.append(rec)

        main_rows.append({
            "call_id": rec["call_id"],
            "transcript_id": rec["transcript_id"],
            "program.program_name": rec["program"]["program_name"],
            "program.program_type": rec["program"]["program_type"],
            "account.account_name": rec["account"]["account_name"],
            "account.business_line": rec["account"]["business_line"],
            "state.state_code": rec["state"]["state_code"],
            "state.region": rec["state"]["region"],
            "channel": rec["channel"],
            "call_type": rec["call_type"],
            "call_outcome": rec["call_outcome"],
            "call_reason": rec["call_reason"],
            "start_time_utc": rec["start_time_utc"],
            "end_time_utc": rec["end_time_utc"],
            "duration_sec": rec["duration_sec"],
            "language": rec["language"],
            "consent_captured": rec["consent_captured"],
            "agent.agent_name": rec["agent"]["agent_name"],
            "intent.primary_intent": rec["intent"]["primary_intent"],
            "intent.sub_intent": rec["intent"]["sub_intent"],
            "disposition": rec["disposition"],
            "sentiment.sentiment_label": rec["sentiment"]["sentiment_label"],
            "sentiment.sentiment_score": rec["sentiment"]["sentiment_score"],
            "urgency_score": rec["urgency_score"],
            "time.start_date": rec["time_features"]["start_date"],
            "time.start_hour": rec["time_features"]["start_hour"],
            "time.start_dow": rec["time_features"]["start_dow"],
            "time.start_week": rec["time_features"]["start_week"],
            "time.start_month": rec["time_features"]["start_month"],
        })

        for t in rec["themes"]:
            themes_rows.append({
                "call_id": rec["call_id"],
                "theme_name": t["theme_name"],
                "theme_level": t["theme_level"],
                "theme_confidence": t["theme_confidence"],
            })

        for tag in rec["tags"]:
            tags_rows.append({"call_id": rec["call_id"], "tag": tag})

        for q in rec["questions"]:
            questions_rows.append({
                "call_id": rec["call_id"],
                "question_text": q["question_text"],
                "occurrences_count": q["occurrences_count"],
            })

        for uq in rec["urgency_quotes"]:
            urgency_rows.append({
                "call_id": rec["call_id"],
                "quote_text": uq["quote_text"],
                "quote_offset_sec": uq["quote_offset_sec"],
                "score": uq["score"],
            })

        for ut in rec["utterances"]:
            utter_rows.append({
                "call_id": rec["call_id"],
                "speaker": ut["speaker"],
                "start_ms": ut["start_ms"],
                "end_ms": ut["end_ms"],
                "text": ut["text"],
                "utterance_sentiment_label": ut["utterance_sentiment_label"],
                "utterance_sentiment_score": ut["utterance_sentiment_score"],
                "toxicity_score": ut["toxicity_score"],
                "contains_question": ut["contains_question"],
            })

    # DataFrames
    df_calls = pd.DataFrame(main_rows)
    df_themes = pd.DataFrame(themes_rows)
    df_tags = pd.DataFrame(tags_rows)
    df_questions = pd.DataFrame(questions_rows)
    df_urgency = pd.DataFrame(urgency_rows)
    df_utter = pd.DataFrame(utter_rows)

    # CSVs
    df_calls.to_csv("calls.csv", index=False)
    df_themes.to_csv("themes.csv", index=False)
    df_tags.to_csv("tags.csv", index=False)
    df_questions.to_csv("questions.csv", index=False)
    df_urgency.to_csv("urgency_quotes.csv", index=False)
    df_utter.to_csv("utterances.csv", index=False)

    # Excel (multi-sheet)
    with pd.ExcelWriter("calls_dataset.xlsx", engine="openpyxl") as xw:
        df_calls.to_excel(xw, index=False, sheet_name="calls")
        df_themes.to_excel(xw, index=False, sheet_name="themes")
        df_tags.to_excel(xw, index=False, sheet_name="tags")
        df_questions.to_excel(xw, index=False, sheet_name="questions")
        df_urgency.to_excel(xw, index=False, sheet_name="urgency_quotes")
        df_utter.to_excel(xw, index=False, sheet_name="utterances")

    # JSONL (full nested)
    with open("calls.jsonl", "w", encoding="utf-8") as f:
        for rec in nested_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("✅ Generated:")
    print("  - CSVs: calls.csv, themes.csv, tags.csv, questions.csv, urgency_quotes.csv, utterances.csv")
    print("  - Excel: calls_dataset.xlsx")
    print("  - JSONL: calls.jsonl")

# ----------------------- Entry point -----------------------

if __name__ == "__main__":
    # adjust n_rows as needed
    generate_dataset(n_rows=200, seed=42)