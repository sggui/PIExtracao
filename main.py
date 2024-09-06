import os
import requests
import json
import time
import random
import logging
import pyodbc

# Configura o logger
logging.basicConfig(
    filename='log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configurações do banco de dados
conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=www.thyagoquintas.com.br,1433;"
    "DATABASE=AUTO;"
    "UID=guilherme_gustavo;"
    "PWD=123456789;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Caminho do arquivo de checkpoint
checkpoint_file = 'checkpoint.json'

def save_checkpoint(page, car_index):
    checkpoint_data = {'page': page, 'car_index': car_index}
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f)
    logging.info(f"Checkpoint salvo: página {page}, carro {car_index}.")

def load_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    return None

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 11; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]

def get_random_user_agent():
    """Retorna um User-Agent aleatório da lista."""
    return random.choice(USER_AGENTS)

def fetch_data(page):
    url = f'https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/carros-usados%2Festoque%3Flkid%3D1000%26estadocidade%3DS%25C3%25A3o%2520Paulo&actualPage={page}&displayPerPage=24&order=1&showMenu=true&showCount=true&showBreadCrumb=true&testAB=false&returnUrl=false&pandora=false'
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'application/json, text/plain, */*',
        'Connection': 'keep-alive',
    }

    response = requests.get(url, headers=headers)
    return response

def fetch_car_details_if_not_exists(car):
    """Verifica se o carro já está no banco de dados e busca detalhes caso não exista."""
    unique_id = car['UniqueId']
    
    if car_exists_in_db(unique_id):
        logging.info(f"Carro com UniqueId {unique_id} já está no banco de dados. Pulando requisição.")
        return None
    else:
        marca = car['Specification']['Make']['Value'].lower().replace(' ', '-')
        modelo = car['Specification']['Model']['Value'].lower().replace(' ', '-')
        especificacao = car['Specification']['Version']['Value'].lower().replace(' ', '-')
        numero_portas = car['Specification']['NumberPorts']
        ano_fabricacao = car['Specification']['YearFabrication']
        ano_modelo = car['Specification']['YearModel']
        
        url_id = f"https://www.webmotors.com.br/api/detail/car/{marca}/{modelo}/{especificacao}/{numero_portas}-portas/{ano_fabricacao}-{ano_modelo}/{unique_id}?pandora=false"
        
        logging.info(f"Buscando detalhes do carro com URL: {url_id}")
        
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'User-Agent': get_random_user_agent()
        }

        response = requests.get(url_id, headers=headers)
        return response

def get_last_page():
    if not os.path.exists('storage'):
        os.makedirs('storage')
        return 1

    json_files = [f for f in os.listdir('storage') if f.endswith('.json')]
    if not json_files:
        return 1
    
    last_file = max(json_files, key=lambda x: int(x.replace('.json', '')))
    last_page = int(last_file.replace('.json', ''))
    
    return last_page + 1

def car_exists_in_db(unique_id):
    """Verifica se o carro com o unique_id já existe no banco de dados."""
    cursor.execute("SELECT COUNT(1) FROM AUTO.dbo.carros WHERE unique_id = ?", (unique_id,))
    result = cursor.fetchone()
    return result[0] > 0

def save_data_to_db(car):
    try:
        unique_id = car['UniqueId']
        title = car['Specification']['Title']
        make = car['Specification']['Make']['Value']
        model = car['Specification']['Model']['Value']
        version = car['Specification']['Version']['Value']
        year_fabrication = car['Specification']['YearFabrication']
        year_model = car['Specification']['YearModel']
        odometer = car['Specification']['Odometer']
        transmission = car['Specification']['Transmission']
        number_ports = int(car['Specification']['NumberPorts']) if car['Specification']['NumberPorts'] else None
        body_type = car['Specification']['BodyType']
        vehicle_attributes = ", ".join([attr['Name'] for attr in car['Specification']['VehicleAttributes']])
        armored = 1 if car['Specification']['Armored'] == "S" else 0
        color = car['Specification']['Color']['Primary']
        seller_type = car['Seller']['SellerType']
        seller_city = car['Seller']['City']
        seller_state = car['Seller']['State']
        tipo_estabelecimento = car['Seller']['AdType']['Value'] if 'AdType' in car['Seller'] and 'Value' in car['Seller']['AdType'] else None
        seller_fantasy_name = car['Seller']['FantasyName']
        car_state = car['Details']['State']
        car_city = car['Seller']['Localization'][0]['City']
        car_country = car['Seller']['Localization'][0]['Country']
        car_neighborhood = car['Seller']['Localization'][0]['Neighborhood']
        car_zip_code = car['Seller']['Localization'][0]['ZipCode']
        car_full_address = car['Seller']['Localization'][0]['AbbrState']
        price_webmotors = car['Prices']['Price']
        price_fipe = car['Details']['FipePrice']
        highest_price = car['Details']['BiggestPrice']
        lowest_price = car['Details']['SmallestPrice']
        average_price = car['Details']['MediumPrice'] 
        car_description = car['LongComment']
        date_update_fipe = car['Details']['UpdateDateFipe'] if 'UpdateDateFipe' in car['Details'] else None
        date_update_webmotors = car['Details']['UpdateDateWebmotorsTable'] if 'UpdateDateWebmotorsTable' in car['Details'] else None

        cursor.execute("""
            INSERT INTO AUTO.dbo.carros (
                unique_id, titulo_carro, marca, modelo, versao, ano_fabricacao, ano_modelo, rodometro,
                cambio_descricao, numero_portas, carroceria_descricao, atributos_veiculo, blindado, cor,
                tipo_vendedor, cidade_vendedor, estado_vendedor, tipo_estabelecimento, nome_fantasia, estado_carro,
                cidade_carro, pais_carro, bairro_carro, cep_carro, endereco_completo_carro, preco_webmotors,
                preco_fipe, maior_preco_encontrado, menor_preco_encontrado, preco_procurado, carro_descricao,
                data_update_fipe, data_update_webmotors
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            unique_id, title, make, model, version, year_fabrication, year_model, odometer,
            transmission, number_ports, body_type, vehicle_attributes, armored, color,
            seller_type, seller_city, seller_state, tipo_estabelecimento, seller_fantasy_name, car_state,
            car_city, car_country, car_neighborhood, car_zip_code, car_full_address, price_webmotors,
            price_fipe, highest_price, lowest_price, average_price, car_description,
            date_update_fipe, date_update_webmotors))

        conn.commit()
        logging.info(f"Carro com UniqueId {unique_id} salvo no banco de dados.")
        return True

    except Exception as e:
        logging.error(f"Erro ao salvar carro no banco de dados: {e}")
        conn.rollback()
        return False

def save_data_to_db_if_not_exists(car):
    """Verifica se o carro já está no banco de dados e salva os dados caso não exista."""
    unique_id = car['UniqueId']
    
    if car_exists_in_db(unique_id):
        logging.info(f"Carro com UniqueId {unique_id} já está no banco de dados. Não será inserido novamente.")
        return False

    return save_data_to_db(car)

def process_and_save_page_data(page_folder):
    # Junta todos os arquivos JSON da página em uma única lista
    all_cars = []
    for json_file in os.listdir(page_folder):
        if json_file.endswith('.json'):
            json_path = os.path.join(page_folder, json_file)
            with open(json_path, 'r', encoding='utf-8') as f:
                car_data = json.load(f)
                all_cars.append(car_data)

    # Salva os dados coletados no banco de dados
    if all_cars:
        for car in all_cars:
            save_data_to_db_if_not_exists(car)

def main():
    checkpoint = load_checkpoint()
    if checkpoint:
        page = checkpoint['page']
        car_start_index = checkpoint['car_index']
        logging.info(f"Retomando do ponto de falha na página {page}, carro {car_start_index}.")
    else:
        page = get_last_page()
        car_start_index = 1

    while True:
        logging.info(f"Fetching data for page {page}...")
        response = fetch_data(page)
        if response.status_code != 200:
            logging.error(f"Erro ao fazer requisição: {response.status_code} - {response.text}")
            break
        
        data = response.json()
        if not data.get('SearchResults'):
            logging.info("Não há mais resultados.")
            break

        # Cria a pasta para a página
        page_folder = os.path.join('storage', f'{page}')
        if not os.path.exists(page_folder):
            os.makedirs(page_folder)

        # Salva o arquivo JSON da página inteira
        page_json_path = os.path.join('storage', f'{page}.json')
        with open(page_json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f'Dados da página {page} salvos com sucesso.')

        total_cars = len(data['SearchResults'])

        # Para cada carro, busque os detalhes e salve em um arquivo separado
        for index, car in enumerate(data['SearchResults'][car_start_index-1:], start=car_start_index):
            unique_id = car.get('UniqueId')

            # Verifica se os detalhes do carro já estão no banco de dados
            details_response = fetch_car_details_if_not_exists(car)
            if details_response is None:
                # Se o carro já existe no banco de dados, continue para o próximo carro
                continue

            if details_response.status_code == 200:
                car['Details'] = details_response.json()
            else:
                logging.error(f"Erro ao buscar detalhes do carro com UniqueId {unique_id}: {details_response.status_code}")
                save_checkpoint(page, index)
                return  # Interrompe o script

            # Salva os dados do carro em um arquivo JSON separado dentro da pasta da página
            car_json_path = os.path.join(page_folder, f'{index}.json')
            with open(car_json_path, 'w', encoding='utf-8') as f:
                json.dump(car, f, indent=4, ensure_ascii=False)

            logging.info(f'Detalhes do carro {index} da página {page} salvos com sucesso.')

            # Salva o checkpoint após cada carro processado com sucesso
            save_checkpoint(page, index)

            # Insere no banco de dados apenas se o carro não estiver lá
            save_data_to_db_if_not_exists(car)

            # Gera um intervalo de espera aleatório entre 5 e 10 segundos entre as requisições de carros
            sleep_time_car = random.randint(5, 10)
            logging.info(f"Aguardando {sleep_time_car} segundos antes de buscar o próximo carro...")
            time.sleep(sleep_time_car)

        # Processa e salva os dados da página inteira no banco de dados
        process_and_save_page_data(page_folder)

        # Verifica se todos os carros da página foram processados
        if index == total_cars:
            car_start_index = 1  # Reinicia o índice de carros para a próxima página
            logging.info(f'Dados da página {page} e seus carros coletados e salvos com sucesso.')

            # Gera um intervalo de espera aleatório entre 15 e 20 segundos entre as requisições de páginas
            sleep_time_page = random.randint(15, 20)
            logging.info(f"Aguardando {sleep_time_page} segundos antes de continuar para a próxima página...")
            time.sleep(sleep_time_page)

            page += 1
        else:
            logging.info(f"A página {page} ainda não foi totalmente processada.")
            save_checkpoint(page, car_start_index)

    logging.info('Todos os dados foram coletados e salvos com sucesso!')

if __name__ == "__main__":
    main()
