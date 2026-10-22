
import random


class QuorumNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.storage = {}
        self.version = {}
        self.is_alive = True
        self.partition = None

    def write_replica(self, key, value, ver):
        if not self.is_alive:
            return False

        self.storage[key] = value
        self.version[key] = ver
        return True

    def read_replica(self, key):
        if not self.is_alive:
            return None, -1

        return (
            self.storage.get(key, None),
            self.version.get(key, 0)
        )


class QuorumCluster:
    def __init__(self, total_nodes=5, n_read=3, n_write=3):
        self.total_nodes = total_nodes
        self.n_read = n_read
        self.n_write = n_write
        self.nodes = [
            QuorumNode(i) for i in range(total_nodes)
        ]

        self._validate_quorum_rules()

    def _validate_quorum_rules(self):
        rule1 = self.n_read + self.n_write > self.total_nodes
        rule2 = self.n_write > self.total_nodes / 2

        print("=== CLUSTER QUORUM INITIALIZATION ===")
        print(
            f"Total Nodes (N): {self.total_nodes} | "
            f"N_R: {self.n_read} | N_W: {self.n_write}"
        )
        print(f"Rule 1 (N_R + N_W > N) -> {rule1}")
        print(f"Rule 2 (N_W > N/2) -> {rule2}")

        if not (rule1 and rule2):
            raise ValueError(
                "Konfigurasi Quorum melanggar aturan Gifford!"
            )

    def client_write(self, key, value, eligible_nodes=None):
        print(f"\n[CLIENT WRITE] {key} = {value}")

        if eligible_nodes is None:
            eligible_nodes = [
                node for node in self.nodes
                if node.is_alive
            ]

        if len(eligible_nodes) < self.n_write:
            print(
                f"[WRITE REJECTED] Node tersedia "
                f"{len(eligible_nodes)}, sedangkan "
                f"N_W = {self.n_write}."
            )
            return False

        write_quorum = random.sample(
            eligible_nodes, self.n_write
        )

        max_ver = max(
            (node.version.get(key, 0)
             for node in write_quorum),
            default=0
        )

        new_ver = max_ver + 1
        written_ids = []

        for node in write_quorum:
            if node.write_replica(key, value, new_ver):
                written_ids.append(node.node_id)

        print(
            f"[WRITE SUCCESS] Node {written_ids}, "
            f"Value: {value}, Version: {new_ver}"
        )
        return True

    def client_read(self, key, eligible_nodes=None):
        print(f"\n[CLIENT READ] Key: {key}")

        if eligible_nodes is None:
            eligible_nodes = [
                node for node in self.nodes
                if node.is_alive
            ]

        if len(eligible_nodes) < self.n_read:
            print(
                f"[READ REJECTED] Node tersedia "
                f"{len(eligible_nodes)}, sedangkan "
                f"N_R = {self.n_read}."
            )
            return None

        read_quorum = random.sample(
            eligible_nodes, self.n_read
        )

        samples = []
        for node in read_quorum:
            value, ver = node.read_replica(key)
            samples.append((node, value, ver))

        # Cari data dengan version tertinggi.
        latest_node, latest_value, latest_ver = max(
            samples, key=lambda item: item[2]
        )

        print(
            "[READ SAMPLES] "
            + str([
                (node.node_id, value, ver)
                for node, value, ver in samples
            ])
        )

        if latest_ver <= 0 or latest_value is None:
            print("[READ RESULT] Data belum ditemukan.")
            return None

        print(
            f"[READ RESULT] Value: {latest_value}, "
            f"Version: {latest_ver}"
        )

        # READ-REPAIR: perbarui node yang tertinggal.
        for node, value, ver in samples:
            if ver < latest_ver:
                if node.write_replica(
                    key, latest_value, latest_ver
                ):
                    print(
                        f"[READ-REPAIR] Node-{node.node_id} "
                        f"diperbarui: Version {ver} -> "
                        f"{latest_ver}, Value: {latest_value}"
                    )

        return latest_value

    def run_original_demo(self):
        print("\n========== PENGUJIAN QUORUM DASAR ==========")

        self.client_write("saldo_rek", 5000000)
        self.client_read("saldo_rek")

        print("\n>>> Node-0 dan Node-1 down <<<")
        self.nodes[0].is_alive = False
        self.nodes[1].is_alive = False

        self.client_write("saldo_rek", 7500000)
        self.client_read("saldo_rek")

        print("\n>>> Node-2 down; tersisa 2 node <<<")
        self.nodes[2].is_alive = False

        self.client_write("saldo_rek", 9000000)
        self.client_read("saldo_rek")

    def run_network_partition_demo(self):
        print("\n========== NETWORK PARTITION ==========")

        # Aktifkan kembali semua node.
        for node in self.nodes:
            node.is_alive = True

        partition_a = self.nodes[0:3]
        partition_b = self.nodes[3:5]

        for node in partition_a:
            node.partition = "A"

        for node in partition_b:
            node.partition = "B"

        print("Partisi A:", [n.node_id for n in partition_a])
        print("Partisi B:", [n.node_id for n in partition_b])

        print("\n--- Write dari Partisi A ---")
        self.client_write(
            "data_partition", "Data dari A",
            eligible_nodes=partition_a
        )

        print("\n--- Write dari Partisi B ---")
        self.client_write(
            "data_partition", "Data dari B",
            eligible_nodes=partition_b
        )

    def run_read_repair_demo(self):
        print("\n========== READ-REPAIR ==========")

        # Siapkan kondisi awal yang terkontrol.
        for node in self.nodes:
            node.is_alive = True
            node.storage.pop("produk", None)
            node.version.pop("produk", None)

        # Empat node memiliki data terbaru.
        for node in self.nodes[0:4]:
            node.write_replica("produk", "Laptop", 2)

        # Node-4 memiliki data versi lama.
        self.nodes[4].write_replica(
            "produk", "Laptop Lama", 1
        )

        print("\nSebelum Read-Repair:")
        for node in self.nodes:
            value, ver = node.read_replica("produk")
            print(
                f"Node-{node.node_id}: "
                f"Value={value}, Version={ver}"
            )

        # Paksa Node-4 ikut Read Quorum agar perbaikan
        # dapat diamati secara konsisten.
        read_nodes = [
            self.nodes[0],
            self.nodes[1],
            self.nodes[4]
        ]

        print("\nMembaca data dari Node-0, Node-1, Node-4:")
        self.client_read("produk", eligible_nodes=read_nodes)

        print("\nSesudah Read-Repair:")
        for node in self.nodes:
            value, ver = node.read_replica("produk")
            print(
                f"Node-{node.node_id}: "
                f"Value={value}, Version={ver}"
            )


def main():
    cluster = QuorumCluster(
        total_nodes=5,
        n_read=3,
        n_write=3
    )

    cluster.run_original_demo()
    cluster.run_network_partition_demo()
    cluster.run_read_repair_demo()


if __name__ == "__main__":
    main()
