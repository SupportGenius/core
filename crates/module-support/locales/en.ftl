# The canned texts `POST /v1/support/messages` shows instead of a model
# answer: the clarify prompt (the answer's confidence fell below the
# tenant's threshold, or a citation did not ground) and the handoff notice
# (the turn was escalated to a person). The English wording is the wording
# these messages have always had; de and ja translate it. Rendered through
# cratefield_i18n, which falls back to this default locale for a turn in
# any other language.
clarify =
    .body = I want to give you an accurate answer rather than a fast wrong one — could you rephrase the question or add a little more detail?
handoff =
    .body = I could not answer this confidently, so I have passed your question to a person who can. You will hear back here.
