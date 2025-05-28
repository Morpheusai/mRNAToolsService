import requests

BASE_URL = "http://localhost:60004"  # 替换成实际暴露的宿主机端口

def test_predict():
    url = f"{BASE_URL}/ImmuneApp"
    payload = {
        "input_file_dir": "minio://molly/aeb1733e-d1d1-4279-bc94-43fc3eee6239_test_peplist.txt",
        "alleles": "HLA-A*01:01,HLA-A*02:01,HLA-A*03:01,HLA-B*07:02",
        "use_binding_score": True,
        "peptide_lengths": [8, 9],
        }
    try:
        response = requests.post(url, json=payload)
        print(f"[predict] status: {response.status_code}, response: {response.text}")
    except Exception as e:
        print(f"[predict] error: {e}")

def test_neo():
    url = f"{BASE_URL}/ImmuneApp_Neo"
    payload = {
        "input_file":"minio://molly/3a39b343-8e2e-4957-8256-55f9bdaae0a6_test_immunogenicity.txt",
        "alleles":"HLA-A*01:01,HLA-A*02:01,HLA-A*03:01,HLA-B*07:02"
        }
    try:
        response = requests.post(url, json=payload)
        print(f"[neo] status: {response.status_code}, response: {response.text}")
    except Exception as e:
        print(f"[neo] error: {e}")
        
def test_transphla():
    url = f"{BASE_URL}/TransPHLA_AOMP"
    payload = {
        "peptide_file": "minio://molly/c2a3fc7e-acdb-483c-8ce4-3532ebb96136_peptides.fasta",
        "hla_file": "minio://molly/29959599-2e39-4a66-a22d-ccfb86dedd21_hlas.fasta",
        "threshold": 0.5,
        "cut_length": 10,
        "cut_peptide": True
    }
    try:
        response = requests.post(url, json=payload)
        print(f"[transphla] status: {response.status_code}, response: {response.text}")
    except Exception as e:
        print(f"[transphla] error: {e}")

if __name__ == "__main__":
    # test_predict()
    # test_neo()
    test_transphla()
