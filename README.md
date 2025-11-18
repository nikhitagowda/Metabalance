MetaBalance – Metabolic Insights From Everyday Data

MetaBalance is a small system I built to show how everyday lifestyle data (steps, sleep, cravings, waist, weight, etc.) can be converted into simple and understandable metabolic insights. The idea is to make metabolic awareness more accessible — without requiring wearables, doctor visits, or expensive trackers.
A user just uploads a CSV and instantly gets a clean dashboard with key indicators and a personalized summary.
This project was developed end-to-end using Google Cloud Run, Gemini, and a lightweight Python backend.

Why I Built This:-
Most people don’t realize that early signals of metabolic slowdown show up in their daily habits long before medical symptoms appear.
Simple things like: Irregular sleep , Frequent cravings, Waist measurements , Steps and activity levels, Mood and stress ,Cycle phase (for women)…all quietly indicate how the body is performing but no tool brings all of this together in a simple, instant way.

MetaBalance attempts to bridge that gap.
It gives users one place to upload basic lifestyle data and receive:
1)A plain-English metabolic summary
2)Early risk indicators (like WHtR)
3)Behavioural patterns
4)A few practical suggestions

The system doesn’t claim to be medical — it’s meant to help users develop awareness and understand their own patterns better.

What the App Does
1. CSV Upload & Preview
Users drop a CSV and can see the first few rows immediately.

2. Automatic Metric Extraction
The backend calculates:
Waist-to-Height Ratio (WHtR)
Average steps
Average sleep
Cravings/alcohol/stress frequency
Weight and waist trends
Metabolic score (simple heuristic)

3. Cloud-run AI Summary
Once the CSV is processed, the backend sends the cleaned data to a separate Cloud Run AI service, which uses Gemini 2.5 Flash to write a concise summary and a few recommendations based on the user’s metrics.

4. Clean Dashboard
The frontend displays:
KPI tiles
CSV preview
Final summary text
Debug payload for development
The UI is intentionally minimal and fast.

High-Level Architecture
Frontend (index.html)
       |
Flask Backend (server.py)
   Stores file in Cloud Storage
   Extracts metrics
   Sends to AI agent
       |
AI Agent (Cloud Run)
   Powered by Gemini
   Returns summary and insights
       |
Frontend displays response

Components used:-Cloud Run , Google Cloud Storage , Cloud Build , Gemini API

Tech Stack
Frontend:
HTML, CSS, JS
Backend:
Python (Flask), Pandas
AI Layer:
Gemini 2.5 Flash (via custom agent service)
Cloud:
Google Cloud Run, Google Cloud Storage, Cloud Build

Future Extensions
Daily,weekly & monthly dashboards in the frontend instead of csv
Wearable integration (Fitbit, Apple Health etc)
Mobile view + progressive web app
