"""Giải nén gói Python 'graphframes' nằm trong jar vào site-packages (dùng lúc build image)."""
import site
import sys
import zipfile

jar = sys.argv[1]
z = zipfile.ZipFile(jar)
names = [n for n in z.namelist() if n.startswith("graphframes/") and n.endswith(".py")]
dest = site.getsitepackages()[0]
for n in names:
    z.extract(n, dest)
print(f"graphframes python: {len(names)} file -> {dest}")
if not names:
    print("CẢNH BÁO: jar không chứa mã Python; job 07 sẽ tự dùng PageRank BSP thay thế.")
