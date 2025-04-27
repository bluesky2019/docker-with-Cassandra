import subprocess
import sys

def manage_cassandra_container():
    # Verifica se o container existe
    container_exists = subprocess.run(
        ["docker", "ps", "-a", "--filter", "name=meu_cassandra", "--format", "{{.Names}}"],
        capture_output=True,
        text=True
    ).stdout.strip() == "meu_cassandra"

    if container_exists:
        resposta = input("Container 'meu_cassandra' já existe. Deseja recriá-lo? (yes/no): ").lower()
        
        if resposta == "yes":
            print("Removendo container existente...")
            subprocess.run(["docker", "rm", "-f", "meu_cassandra"], check=True)
        elif resposta == "no":
            print("Utilizando container existente...")
            # Verifica se está rodando
            status = subprocess.run(
                ["docker", "ps", "--filter", "name=meu_cassandra", "--format", "{{.Status}}"],
                capture_output=True,
                text=True
            ).stdout.strip()
            
            if not status:
                print("Iniciando container...")
                subprocess.run(["docker", "start", "meu_cassandra"], check=True)
            
            enter_cassandra_shell()
            return
        else:
            print("Opção inválida. Saindo.")
            return

    # Inicia novo container
    try:
        print("Iniciando novo container Cassandra...")
        subprocess.run([
            "docker", "run", "-d",
            "--name", "meu_cassandra",
            "-p", "9042:9042",  # Porta do CQL
            "-p", "9160:9160",  # Porta Thrift
            "cassandra:latest"
        ], check=True)
        
        print("\nContainer iniciado com sucesso!")
        print("Aguardando Cassandra inicializar...")
        
        # Espera o Cassandra ficar pronto
        subprocess.run([
            "docker", "exec", "meu_cassandra",
            "bash", "-c", "until cqlsh -e 'describe cluster' > /dev/null 2>&1; do sleep 2; done"
        ], check=True)
        
        enter_cassandra_shell()
        
    except subprocess.CalledProcessError as e:
        print(f"Erro: {e.stderr}")
        sys.exit(1)

def enter_cassandra_shell():
    print("\nIniciando sessão CQLSH...")
    print("Comandos básicos do Cassandra:")
    print(" - CREATE KEYSPACE...")
    print(" - CREATE TABLE...")
    print(" - INSERT INTO...")
    print(" - SELECT * FROM...")
    print(" - DROP TABLE...")
    print("Digite 'EXIT' para sair\n")
    
    # Entra no shell interativo
    subprocess.run([
        "docker", "exec", "-it",
        "meu_cassandra",
        "cqlsh"
    ])

if __name__ == "__main__":
    manage_cassandra_container()