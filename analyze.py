#!/usr/bin/env python3
"""
Hook classification + Ménagénie-adapted script generation via the
Claude API. Called from refresh.py's weekly pipeline: classify_hook()
is cheap and applied to every transcribed reel/video; generate_adaptation()
is pricier and applied only to the week's top 7 outliers (the ones that
also feed the weekly digest issue).

Requires ANTHROPIC_API_KEY env var. If it's unset, every function here
returns None so the rest of the pipeline degrades gracefully instead
of failing — hook classification and script generation are additive,
not load-bearing.
"""
import json
import os
import urllib.error
import urllib.request

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

# Same 13-category taxonomy as the spoken-hook generator at
# menagenie/hub-rapports/reports/generateur-script-video.html — reusing
# it keeps both tools speaking the same language, so a hook type found
# here can deep-link straight into that tool.
SPOKEN_HOOK_TYPES = [
    "Secret Reveal / Breakdown", "Case Study", "Problem", "Contrarian",
    "Negative", "Education", "List", "Scenario/Hypothetical", "Comparison",
    "Question", "Ranking/Rating", "Authority", "Personal Experience",
]

MENAGENIE_BRAND_CONTEXT = (
    "Ménagénie est une entreprise d'entretien ménager résidentiel et "
    "commercial au Québec (Montréal, Laval, Rive-Sud). Slogan : "
    "\"Le ménage parfait. Zéro gestion. Zéro stress.\" Ton : direct, "
    "rassurant, orienté résultat — pas de jargon corporate."
)


def _call_claude(system, user_message, max_tokens=300):
    if not ANTHROPIC_API_KEY:
        return None
    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user_message}],
    }
    req = urllib.request.Request(
        ANTHROPIC_URL,
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
        return "".join(b.get("text", "") for b in data.get("content", [])).strip() or None
    except (urllib.error.URLError, urllib.error.HTTPError) as e:
        print(f"  ! Claude API call failed: {e}")
        return None


def is_relevant(caption, transcript):
    """Is this post actually about residential/commercial cleaning, home
    organization, or a closely related niche? Multi-topic lifestyle
    influencers (parenting, decor, recipes...) post plenty of content
    that has nothing to do with cleaning even though the account itself
    was added as a cleaning-niche creator.

    Defaults to True (keep) when there's no usable text or no API key —
    this must never penalize accounts that just have weak captioning
    (e.g. small local competitors relying on trending audio with no
    caption at all); it only screens out posts with a clear off-topic
    signal in the text."""
    text = f"{caption or ''}\n{transcript or ''}".strip()
    if not text or not ANTHROPIC_API_KEY:
        return True
    system = (
        "Tu juges si un post/reel est pertinent pour un outil de veille "
        "concurrentielle sur l'entretien ménager (nettoyage résidentiel "
        "ou commercial, organisation, rangement de la maison). Réponds "
        "UNIQUEMENT par 'oui' ou 'non'. Si le sujet est clairement autre "
        "chose (parentalité, mode, recettes, business général sans lien "
        "avec le ménage, etc.), réponds 'non'. Si le texte est ambigu, "
        "trop court, ou ne permet pas de juger, réponds 'oui' (bénéfice "
        "du doute)."
    )
    result = _call_claude(system, text[:500], max_tokens=5)
    if not result:
        return True
    return "non" not in result.lower()[:10]


def is_cleaning_service_account(bio, full_name):
    """Is this Instagram account actually a residential/commercial
    cleaning SERVICE business (someone Ménagénie competes with or takes
    inspiration from) — not a raw-materials supplier, a cleaning-product
    brand, a business-coaching/franchise academy, a car-detailing
    business, or something merely bio-keyword-adjacent? Used by
    discover.py's account-level guardrail; keyword matching alone let
    through a cleaning-supplies manufacturer and an auto-detailing
    coaching account, since both bios happened to contain "cleaning"/
    "nettoyage". Defaults to True (keep) when there's no API key, so
    discovery still works (just less precisely) without one."""
    text = f"{full_name or ''}\n{bio or ''}".strip()
    if not text or not ANTHROPIC_API_KEY:
        return True
    system = (
        "Tu juges si un compte Instagram est une VRAIE entreprise de "
        "service de nettoyage résidentiel ou commercial (quelqu'un qui "
        "nettoie des maisons, condos ou bureaux pour des clients) — pas "
        "un fournisseur de produits ou matières premières de nettoyage, "
        "pas une académie/coaching business qui vend des formations sur "
        "\"comment lancer une entreprise de nettoyage\", pas un service "
        "de nettoyage automobile/de voitures, et pas un compte sans "
        "rapport réel. Réponds UNIQUEMENT par 'oui' ou 'non'."
    )
    result = _call_claude(system, text[:400], max_tokens=5)
    if not result:
        return True
    return "non" not in result.lower()[:10]


def classify_hook(transcript):
    """Return one of SPOKEN_HOOK_TYPES, or None if classification fails
    or there's no transcript to classify."""
    if not transcript or not transcript.strip() or not ANTHROPIC_API_KEY:
        return None
    system = (
        "You classify the opening hook of a short-form video transcript "
        "into exactly one category. Reply with ONLY the category name, "
        "nothing else. Categories: " + ", ".join(SPOKEN_HOOK_TYPES)
    )
    result = _call_claude(system, transcript[:600], max_tokens=20)
    if not result:
        return None
    for cat in SPOKEN_HOOK_TYPES:
        if cat.lower() in result.lower():
            return cat
    return None



# Substrings that mean Claude asked a clarifying question or refused
# instead of producing a script — happens on very short/low-signal
# transcripts (e.g. "No, hello?"). Treat as a failed generation rather
# than caching garbage onto the post forever.
_REFUSAL_MARKERS = [
    "je ne vois pas", "pourriez-vous", "pouvez-vous", "peux-tu préciser",
    "as-tu plus de", "manque d'information", "besoin de plus",
    "i don't see", "could you provide", "can you share", "can you clarify",
]


def generate_adaptation(transcript, hook_type):
    """Draft a short Ménagénie-branded opening line inspired by this
    outlier's hook mechanism. Returns None on failure (including when
    the model asks a clarifying question instead of writing the script)."""
    if not transcript or not transcript.strip() or not ANTHROPIC_API_KEY:
        return None
    system = (
        f"{MENAGENIE_BRAND_CONTEXT}\n\n"
        f"On te donne la transcription d'un reel qui a surperformé chez un "
        f"concurrent, classé comme hook de type \"{hook_type}\". Écris une "
        f"ouverture de 2-3 phrases (en français québécois, adaptée à "
        f"Ménagénie) qui reprend le MÊME mécanisme d'accroche sur un sujet "
        f"pertinent au ménage résidentiel ou commercial. Ne traduis pas "
        f"littéralement — adapte l'angle. Même si la transcription fournie "
        f"est très courte ou ambiguë, travaille avec le peu que tu as — "
        f"inspire-toi du STYLE et du RYTHME du hook plutôt que de son "
        f"contenu littéral. Ne pose JAMAIS de question de clarification et "
        f"ne demande jamais plus de contexte : réponds uniquement avec le "
        f"script final, sans préambule ni guillemets."
    )
    result = _call_claude(system, transcript[:600], max_tokens=200)
    if result and any(marker in result.lower() for marker in _REFUSAL_MARKERS):
        return None
    return result
