import requests

url = "http://localhost:60002/"

def test_netchop():
    test_url = url + "netchop"
    payload = {
        "input_file": "minio://molly/6b5a0a9b-4dc3-420d-b53d-a4ca375c51d1_testSeq.fsa",
        "cleavage_site_threshold": 0.5
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

def test_netctlpan():
    test_url = url + "netctlpan"
    payload = {
        "input_file": "minio://molly/6b5a0a9b-4dc3-420d-b53d-a4ca375c51d1_testSeq.fsa",
        "mhc_allele": "HLA-A02:01",
        "weight_of_clevage": 0.225,
        "weight_of_tap": 0.025,
        "peptide_lengt": "8,9,10,11"
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

def test_netmhcpan():
    test_url = url + "netmhcpan"
    payload = {
        "input_file": "minio://molly/6b5a0a9b-4dc3-420d-b53d-a4ca375c51d1_testSeq.fsa",
        "mhc_allele": "HLA-A02:01",
        "high_threshold_of_bp": 0.5,
        "low_threshold_of_bp": 2.0,
        "peptide_lengt": "8,9,10,11"
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

def test_netmhcstabpan():
    test_url = url + "netmhcstabpan"
    payload = {
        "input_file": "minio://molly/d530f953-5be7-42bb-a170-144033a2eb92_test_netchop.fsa",
        "mhc_allele": "HLA-A02:01",
        "high_threshold_of_bp": 0.5,
        "low_threshold_of_bp": 2.0,
        "peptide_lengt": "8,9,10,11"
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

def test_nettcr():
    test_url = url + "nettcr"
    payload = {
        "input_file": "minio://molly/56ed1020-2674-4227-b60a-024bdefbc5dd_small_example.xlsx",
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

def test_bigmhc():
    test_url = url + "bigmhc"
    payload = {
        "input_file": "minio://molly/870d243f-04e7-4bc7-8089-0ff911bbd666_example1.csv",
        "model_type": "el"
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

def test_prime():
    test_url = url + "prime"
    payload = {
        "input_file": "minio://molly/8b787433-88fb-41ac-8ec7-302e06606731_test.txt",
        "mhc_allele": "B1801"
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"


if __name__ == "__main__":
    #test_netchop()
    #test_netctlpan()
    #test_netmhcpan()
    # test_netmhcstabpan()
    # test_nettcr()
    test_bigmhc()
    # test_prime()
    print("Test passed!")

