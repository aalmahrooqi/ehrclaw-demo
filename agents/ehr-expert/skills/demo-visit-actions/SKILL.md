---
name: demo-visit-actions
description: Prepare simulated follow-up, prescription, X-ray, and blood-test request cards directly in the CliniClaw patient chat. During clinician-plan discussions, proactively offer concrete next steps, including three available appointment times when arranging follow-up. After a focused blood-result or monitoring question reveals a relevant testing gap, offer a blood-test request for clinician review. Keep initial summaries free of action offers. Use explicit clinician instructions for order details; suggestions do not authorize treatment. The dashboard owns local confirmation, cancellation, and labelling. Never contact real booking, pharmacy, laboratory, imaging, or FHIR write services.
---

# Visit actions in chat

Speak your suggestions naturally in the conversation. The dashboard renders
supporting cards beneath your reply; it immediately shows appointment times
without a discovery button. The clinician can book by replying in chat, and can
review, edit, confirm or dismiss other drafts in the same conversation. Do not send the user to a separate Visit actions tab. The app owns the
simulated ledger; you cannot create a real order or book through another tool.

## User-facing wording

The app already labels the overall experience as a demo. In follow-up and
blood-test replies, use plain labels such as “follow-up appointment”, “Follow-up
clinic” and “blood-test request”; do not repeat demo, fictional or simulated.
Display appointment and lab references as `APT-…` and `LAB-…`, omitting any
internal `DEMO-` prefix. This is only a wording convention; the local-only
workflow and confirmation rules remain in force.

## Match the visit stage

The demo proceeds from understanding the case to making the plan, then carrying
it out. Generating or regenerating a handoff ends at its four summary sections,
without this skill or action offers. In patient Q&A, answer the evidence question
first. Offer relevant next steps during clinician-plan discussions, and after a
focused blood-result or monitoring question reveals an unresolved testing gap.
Routine chart, medication and drug-safety lookups without a relevant gap or plan
do not need offers. An intake mention or old chart order alone is not a trigger.
Explicit requests for actions still work whenever the clinician makes them.

### When a blood-result question reveals a gap

If the current clinician question concerns blood results or monitoring and the
retrieved records show only historical results, no newer results found, or a
documented uncompleted test relevant to that question, explain the gap with
source dates and citations, then offer a lab-request draft. For example:
“The latest kidney results I found are dated 2019-06-29. Would you like me to
prepare a repeat blood-test request for your review?” Do not wait for the
clinician to request an order before offering this optional next step.

Use `suggestions: [{"type":"lab","evidence":"exact current clinician question"}]`
and `actions: []`. The question grounds the relevance of the offer; it does not
authorize an order. Age alone does not establish that testing is overdue or
medically necessary. Do not invent retesting intervals, choose tests, or claim a
lab appointment has been booked. A clear affirmative reply requests a draft;
leave unspecified tests and reason blank, rather than copying names from old
results as though the clinician selected them. Respect dismissed/active offers
and immediate-attention restrictions as usual.

## Answer first, offer relevant options

- Answer the current question with the appropriate evidence skill. Drug-safety
  questions still require `drug-label-interactions` and its packaged script.
  Existing blood results require read-only FHIR retrieval with dates, units and
  source IDs. Use the dashboard current date for recency; prefer absolute result
  dates to guessed elapsed years/months. Limit “no newer results found” to the
  records actually searched. A lab request is not a result: never fabricate values
  or normality.
- When the conversation makes a next workflow step useful, offer at most TWO
  optional suggestions. A clinician plan to request blood tests can offer
  a blood-test request draft; discussion of a next appointment or follow-up
  should immediately offer the next three available times; an
  explicit medicine plan can offer a prescription draft; confirmed visit actions
  can offer the patient summary. Do not add a generic menu after every reply.
- Include the actual suggestion and a useful next question in `reply`, not just
  metadata or an instruction to click a button. For example, ask whether to
  prepare a lab request and ask which tests the clinician wants. A clear “Yes”
  to a single draft offer is an explicit request; quote that reply as instruction.
- Suggestions are choices to prepare a draft, not new clinical recommendations.
  Do not select a drug, dose, test, imaging site, or treatment on your own. Avoid
  routine follow-up suggestions during unresolved immediate-attention scenarios.
- Ground each suggestion in an exact quote from a clinician message or optional
  clinician notes. For the testing-gap offer above, quote the current focused
  question and explain the retrieved gap in `reply`; other offers need intent for
  the next step. Do not use intake, the generated handoff, chart facts or assistant
  proposals as the sole trigger.
- Do not repeat active or dismissed offers, or types with an active
  draft/confirmed action. If a prior follow-up booking was cancelled and the
  clinician asks to arrange follow-up again, return a fresh `follow_up` suggestion
  with the current available dates. An old accepted offer does not block that
  new request. Explicit requests still work.
- A proactive suggestion belongs in `suggestions`, not `actions`. Offering it
  does not create an order. Dismissing it does not change the care plan.

## Conversational appointments

When the clinician starts discussing arranging a next appointment or follow-up,
proactively return
a `follow_up` suggestion with an exact evidence quote. Introduce the available
times and ask which works. The app immediately displays its next three available
fictional slots as a numbered list, in clinic local time. Do not duplicate the
list, invent dates or make the clinician click “Find follow-up dates” first.

The clinician can reply “Book the second one”, “Option 2”, or “Book YYYY-MM-DD
at HH:MM”. The app resolves the displayed choice, rechecks availability, and
records a local demo booking with a reference. A vague “yes” needs a specific
time. If another booking takes the slot, the app offers three fresh choices.
Only the resulting ledger record establishes a booking; never claim success
from chat text alone. If a booking request reaches you without a confirmed
ledger record, ask the clinician to use “Book option 1/2/3”.

Use only the supplied calendar. If a clinician’s requested interval is not
represented by those slots, explain that gap and ask whether an offered time
works. Do not imply a date meets an interval when it does not.

## Explicit action requests

- Copy the patient ID exactly and use only this patient's context.
- Prepare action cards only on an explicit clinician request. A question,
  hypothetical, old order or assistant suggestion is not a clinician decision.
- Copy action details verbatim from clinician messages, optional clinician notes,
  or clinician-edited draft fields. Preserve doses, route, frequency, duration,
  body side and test names. Do not normalize or infer missing details.
- Missing details are empty strings. The card provides fields for the clinician
  to fill in. An explicit draft request still returns a card even when all
  required details are missing; do not respond with only a clarification question.
  Ask briefly for those details in reply; do not make the choice yourself.
- The UI supplies fictional appointment slots; never invent dates or imply a
  requested interval is available. The clinician chooses a displayed slot by chat or on the card.
- `lab` is a blood-test request with tests and a reason. It does not record sample
  collection or results. `xray` is a request, not an acquired image or report.
  Uploaded chest films belong to `nv-reason-cxr`, not this workflow.
- Saving a prescription draft does not verify medication safety or send it.
- The clinician can cancel using the existing card. Do not claim cancellation,
  transmission, dispensing, testing or treatment occurred from chat text. A booking
  is confirmed only when it is present in the supplied local ledger.
- Do not use terminal, browser, file tools, FHIR write skills or external APIs for
  these simulated actions. Ordinary read-only evidence retrieval remains separate.

## Response contract

When the dashboard requests its patient-chat envelope, always return ONLY one
JSON object, with empty arrays when no action or suggestion fits. Keep the
complete answer, including retrieved evidence citations, in `reply`.

Use valid JSON with double-quoted keys and strings, without surrounding prose or
code fences. `reply` must be nonempty clinician-facing Markdown only: never put a
second JSON object, schema example, tool call, or action metadata inside `reply`.
Keep all action metadata in the top-level `actions` and `suggestions` arrays.
For example:

```json
{
  "patientId": "copy the provided ID",
  "reply": "Answer the clinician's question here.",
  "actions": [],
  "suggestions": [
    {"type": "lab", "evidence": "Exact quote from the supplied discussion"}
  ]
}
```

`actions`: at most four `{type, instruction, details}` objects. `instruction`
must quote an explicit clinician request. Allowed detail keys (all strings):

- `follow_up`: `reason`.
- `prescription`: `medication`, `dose`, `route`, `frequency`, `duration`,
  `quantity`, `pharmacy`. The first five are required before confirmation.
- `xray`: `bodySite`, `reason`. Both are required before confirmation.
- `lab`: `tests`, `reason`. Both are required before confirmation. `tests` is one
  string containing the requested blood tests, e.g. the clinician's exact list.

Every nonempty detail must occur verbatim in a clinician source. Use empty
strings for missing details. Do not return IDs, statuses, references, results,
code or tool commands; the app generates these records after validation.

`suggestions`: at most two `{type, evidence}` objects. Allowed types are
`follow_up`, `prescription`, `xray`, `lab`, `after_visit`. Evidence is an exact
8–500 character quote from the current focused question for a testing-gap offer,
or from clinician messages/notes indicating the planned next step. Do not add
order details.

For a handoff, follow its dedicated four-section contract and offer no workflow
actions. If the dashboard requests the JSON envelope, put only the handoff in
`reply` and leave both arrays empty. In dashboard Q&A, use empty arrays when
there is no relevant testing-gap offer or planned next step. Never sacrifice the answer or its
evidence citations to offer a next step.
