import requests

url = "http://localhost:60002/"


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

    # test_prime()
    print("Test passed!")

