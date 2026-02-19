import os
import subprocess


def start_gpg_encrypt_pipe(gpg_key: bytes, stdin_pipe: int):
    pread, pwrite = os.pipe()
    call_args = [
        "gpg", "--symmetric", "--no-symkey-cache",
        "--cipher-algo", "AES256", "--digest-algo", "SHA256", "--cert-digest-algo", "SHA256",
        "--no-compress",
        "--s2k-mode", "3", "--s2k-count", "65011712", "--force-mdc",
        "--batch", "--pinentry-mode", "loopback", "--passphrase-fd", str(pread)
    ]

    p = subprocess.Popen(call_args, stdin=stdin_pipe, stdout=subprocess.PIPE, pass_fds=(pread,))
    os.close(pread)
    os.write(pwrite, gpg_key)
    os.close(pwrite)
    return p


def start_gpg_decrypt_pipe(gpg_key: bytes, stdin_pipe: int):
    pread, pwrite = os.pipe()
    call_args = [
        "gpg", "--decrypt", "--no-symkey-cache",
        "--batch", "--pinentry-mode", "loopback", "--passphrase-fd", str(pread)
    ]

    p = subprocess.Popen(call_args, stdin=stdin_pipe, stdout=subprocess.PIPE, pass_fds=(pread,))
    os.close(pread)
    os.write(pwrite, gpg_key)
    os.close(pwrite)
    return p


def gpg_encrypt_file(input_path: str, output_path: str, gpg_key: bytes):
    with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
        p = start_gpg_encrypt_pipe(gpg_key, fin.fileno())
        while True:
            chunk = p.stdout.read(4 * 1024 * 1024) # type: ignore
            if not chunk:
                break
            fout.write(chunk)
        p.wait()


def gpg_decrypt_file(input_path: str, output_path: str, gpg_key: bytes):
    with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
        p = start_gpg_decrypt_pipe(gpg_key, fin.fileno())
        while True:
            chunk = p.stdout.read(4 * 1024 * 1024) # type: ignore
            if not chunk:
                break
            fout.write(chunk)
        p.wait()
