import sys
sys.path.insert(0, '.')
from app import fetch_campaign_insights, extract_product_name, sum_actions

token = 'EAASKghWluokBRDZChZAaEvZBlBQ0D1iR7oi3EzrwChPlDNer3ziIYRN92TpkOuZBIyfZBoFMZAGLqAZBNhlE6R5WUZBPcaFGjaZCFuhjtQJoP1qmf9ZCjstOlQvDWdxZAlSAmUWBrU1oeXiRFftHem4Cwavusvm5y3Et2NC60HZCiyFKo2J6AOM7ys3vXblRPopnfdlkXgZDZD'

campaigns = fetch_campaign_insights('1405818800844826', token, 'today')
print(f'Tong so campaign: {len(campaigns)}')
total_data = 0
for i, c in enumerate(campaigns, 1):
    name = c.get('campaign_name', '')
    product = extract_product_name(name)
    actions = c.get('actions', [])
    data = sum_actions(actions)
    spend = float(c.get('spend', 0))
    if spend > 0:
        print(f'\n[{i}] Campaign: {name}')
        print(f'    Product: {product}')
        print(f'    Actions: {actions}')
        print(f'    Data (sum): {data}')
        print(f'    Spend: {spend}')
        total_data += data
print(f'\nTotal data: {total_data}')
