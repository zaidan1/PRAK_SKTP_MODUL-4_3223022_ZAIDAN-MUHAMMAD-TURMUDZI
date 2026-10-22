import time


class ReplicaNode:
    def __init__(self, node_id, is_primary=False):
        self.node_id = node_id
        self.is_primary = is_primary
        self.is_alive = True
        self.storage = {}
        self.version = {}

    def update_data(self, key, value, ver):
        if not self.is_alive:
            return False

        self.storage[key] = value
        self.version[key] = ver

        role = "PRIMARY" if self.is_primary else "BACKUP"

        print(
            f"[{role}-{self.node_id}] Berhasil simpan "
            f"{key}='{value}' (Version: {ver})"
        )

        return True


class PrimaryBackupSystem:
    def __init__(self):
        self.primary = ReplicaNode(
            node_id=0,
            is_primary=True
        )

        self.backups = [
            ReplicaNode(node_id=1, is_primary=False),
            ReplicaNode(node_id=2, is_primary=False)
        ]

    def write(self, key, value):
        print(
            f"\n--- [CLIENT WRITE] Request: "
            f"set {key} = '{value}' ---"
        )

        if not self.primary.is_alive:
            print(
                "[ERROR] Primary Node mati! "
                "Operasi Write ditolak."
            )
            return False

        current_ver = self.primary.version.get(key, 0)
        new_ver = current_ver + 1

        # 1. Update Primary
        self.primary.update_data(
            key,
            value,
            new_ver
        )

        # 2. Replikasi sinkron ke Backup
        ack_count = 0

        for backup in self.backups:
            if backup.is_alive:
                time.sleep(0.5)

                if backup.update_data(
                    key,
                    value,
                    new_ver
                ):
                    ack_count += 1
            else:
                print(
                    f"[WARNING] Backup-{backup.node_id} "
                    f"unreachable/down!"
                )

        print(
            f"[WRITE STATUS] Diterima {ack_count} ACK "
            f"dari total {len(self.backups)} backup."
        )

        return True

    def read(self, key, from_node="primary"):
        print(
            f"\n--- [CLIENT READ] Request: "
            f"get {key} from {from_node.upper()} ---"
        )

        target = (
            self.primary
            if from_node == "primary"
            else self.backups[0]
        )

        if not target.is_alive:
            print(
                f"[ERROR] Node {target.node_id} sedang down!"
            )
            return None

        val = target.storage.get(
            key,
            "KEY_NOT_FOUND"
        )

        ver = target.version.get(key, 0)

        print(
            f"[READ RESULT] {key} = '{val}' "
            f"(Version: {ver})"
        )

        return val


def run_primary_backup_demo():
    cluster = PrimaryBackupSystem()

    # Transaksi normal
    cluster.write(
        "user_101",
        "Ahmad Fauzi"
    )

    cluster.read(
        "user_101",
        from_node="primary"
    )

    cluster.read(
        "user_101",
        from_node="backup"
    )

    # Skenario 1:
    # Salah satu backup mengalami crash
    print(
        "\n>>> Skenario: Backup-2 "
        "mengalami hardware crash <<<"
    )

    cluster.backups[1].is_alive = False

    cluster.write(
        "user_102",
        "Siti Nurhaliza"
    )

    # Skenario 2:
    # Primary mengalami crash
    print(
        "\n>>> Skenario: Primary Node "
        "mengalami server failure <<<"
    )

    cluster.primary.is_alive = False

    cluster.write(
        "user_103",
        "Budi Santoso"
    )


if __name__ == '__main__':
    run_primary_backup_demo()