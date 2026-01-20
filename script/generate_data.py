import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_clients(n_clients: int, output_path: str) -> list[int]:
    """
    Generate synthetic client data and save to a CSV file.
    
    :param n_clients: Description
    :type n_clients: int
    :param output_path: Description
    :type output_path: str
    :return: Description
    :rtype: list[int]
    """
    countries = ["France", "Germany", "Spain", "Italy", "Belgium", "Netherlands", "Sweden", "UK", "Canada"]
    
    clients = []
    client_ids = []
    
    for i in range(1, n_clients + 1):
        date_inscription = fake.date_between(start_date='-3y', end_date='-1m')
        clients.append({
            "id_client": i,
            "nom": fake.name(),
            "email": fake.email(),
            "date_inscription": date_inscription.strftime("%Y-%m-%d"),
            "pays": random.choice(countries)
        })
        client_ids.append(i)
        
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["id_client", "nom", "email", "date_inscription", "pays"])
        writer.writeheader()
        writer.writerows(clients)
        
    print(f"Generated {n_clients} clients to {output_path}")
    return client_ids

def generate_achats(client_ids: list[int], avg_purchase_per_clients: int, output_path: str) -> None:
    """
    Generate fake purchase data and save to a CSV file.

    Args:
        client_ids (list[int]): List of client IDs with id_achat, id_client, data_achat, montant, produit.
        avg_purchase_per_clients (int): Average number of purchases per client.
        output_path (str): Path to save the generated CSV file.
    """

    products = ["Laptop", "Phone", "Tablet", "Headphones", "Smartwatch", "Camera", "Printer", "Monitor", "Keyboard", "Mouse"]

    achats = []
    id_achat = 1

    for id_client in client_ids:
        n_purchases = random.randint(1, avg_purchase_per_clients * 2)

        for _ in range(n_purchases):
            date_achat = fake.date_between(start_date='-2y', end_date='today')
            achats.append({
                "id_achat": id_achat,
                "id_client": id_client,
                "date_achat": date_achat.strftime("%Y-%m-%d"),
                "montant": round(random.uniform(10.0, 2000.0), 2),
                "produit": random.choice(products)
            })
            id_achat += 1

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["id_achat", "id_client", "date_achat", "montant", "produit"])
        writer.writeheader()
        writer.writerows(achats)

    print(f"Generated {len(achats)} purchases to {output_path}")

if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    clients_ids = generate_clients(1500, output_dir / "clients.csv")
    generate_achats(clients_ids, 5, output_dir / "achats.csv")