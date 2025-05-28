import requests

url = "http://localhost:60005/"


def test_piste():
    test_url = url + "piste"
    payload = {
        "input_file_dir_minio":"minio://molly/39e012fc-a8ed-4ee4-8a3b-092664d72862_piste_example.csv",
        "model_name":"unipep",
        "threshold": 0.5,
        "antigen_type": "MT"
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

def test_pmtnet():
    test_url = url + "pMTnet"
    payload = {
        "input_file_dir_minio":"minio://molly/66dd7c86-f1c4-455e-9e50-3b2a77be66c9_test_input.csv",
    }
    response = requests.post(test_url, json=payload)
    print(response.text)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

if __name__ == "__main__":
    # test_piste()
    test_pmtnet()
    print("Test passed!")

