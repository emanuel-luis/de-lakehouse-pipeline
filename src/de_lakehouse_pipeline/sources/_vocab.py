"""Static vocabulary for the synthetic generator."""

from typing import Final

#: (city, UF, weight): every UF capital plus three extra cities; SP and RJ dominate.
CITIES: Final[tuple[tuple[str, str, int], ...]] = (
    ("rio branco", "AC", 1),
    ("maceio", "AL", 1),
    ("manaus", "AM", 1),
    ("macapa", "AP", 1),
    ("salvador", "BA", 2),
    ("fortaleza", "CE", 1),
    ("brasilia", "DF", 3),
    ("vitoria", "ES", 1),
    ("goiania", "GO", 2),
    ("sao luis", "MA", 1),
    ("belo horizonte", "MG", 5),
    ("campo grande", "MS", 1),
    ("cuiaba", "MT", 1),
    ("belem", "PA", 1),
    ("joao pessoa", "PB", 1),
    ("recife", "PE", 2),
    ("teresina", "PI", 1),
    ("curitiba", "PR", 4),
    ("rio de janeiro", "RJ", 12),
    ("natal", "RN", 1),
    ("porto velho", "RO", 1),
    ("boa vista", "RR", 1),
    ("porto alegre", "RS", 4),
    ("florianopolis", "SC", 3),
    ("aracaju", "SE", 1),
    ("sao paulo", "SP", 30),
    ("palmas", "TO", 1),
    ("campinas", "SP", 5),
    ("niteroi", "RJ", 2),
    ("uberlandia", "MG", 2),
)

UFS: Final = tuple(sorted({uf for _, uf, _ in CITIES}))  # all 27

#: (Portuguese name, English translation); ``None`` is untranslated on purpose.
CATEGORIES: Final[tuple[tuple[str, str | None], ...]] = (
    ("beleza_saude", "health_beauty"),
    ("informatica_acessorios", "computers_accessories"),
    ("automotivo", "auto"),
    ("cama_mesa_banho", "bed_bath_table"),
    ("moveis_decoracao", "furniture_decor"),
    ("esporte_lazer", "sports_leisure"),
    ("perfumaria", "perfumery"),
    ("utilidades_domesticas", "housewares"),
    ("telefonia", "telephony"),
    ("relogios_presentes", "watches_gifts"),
    ("brinquedos", "toys"),
    ("cool_stuff", "cool_stuff"),
    ("ferramentas_jardim", "garden_tools"),
    ("bebes", "baby"),
    ("eletronicos", "electronics"),
    ("papelaria", "stationery"),
    ("livros_interesse_geral", "books_general_interest"),
    ("pet_shop", "pet_shop"),
    ("moveis_escritorio", "office_furniture"),
    ("pc_gamer", None),
)

STATUSES: Final = (
    ("delivered", 92.0),
    ("shipped", 2.0),
    ("invoiced", 1.5),
    ("processing", 1.5),
    ("approved", 0.5),
    ("canceled", 1.5),
    ("unavailable", 0.7),
    ("created", 0.3),
)
#: Statuses an order may have when it has no items.
UNFULFILLED: Final = ("canceled", "unavailable", "created")

PAYMENT_TYPES: Final = (
    ("credit_card", 74.0),
    ("boleto", 19.0),
    ("voucher", 5.0),
    ("debit_card", 1.5),
)

REVIEW_MESSAGES: Final = (
    "Produto chegou antes do prazo",
    "Recomendo a loja",
    "Veio diferente do anunciado, mas funciona",
    "Entrega demorou, produto ok",
    "Qualidade abaixo do esperado",
    "Tudo certo, obrigado",
)
