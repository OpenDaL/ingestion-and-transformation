# -*- coding: utf-8 -*-
from metadata_ingestion.translate import publisher

payload = {'organization': 'Комунальне некомерційне підприємство "Бурштинський міський центр первинної медико-санітарної допомоги" Бурштинської міської ради '}
print(publisher(payload))