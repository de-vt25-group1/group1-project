import requests
from urllib.parse import quote
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def stream_download_to_hdfs(download_url, hdfs_path, namenode_host="localhost", namenode_port=9870, user="hdfs"):
    """
    Stream download directly to HDFS without storing locally.
    
    Args:
        download_url: URL to download the data from
        hdfs_path: Destination path in HDFS
        namenode_host: HDFS NameNode hostname
        namenode_port: WebHDFS port (default: 9870)
        user: HDFS user
    """
    # Encode the HDFS path for URL
    encoded_hdfs_path = quote(hdfs_path)
    
    # WebHDFS URL for creating a file with data
    webhdfs_url = f"http://{namenode_host}:{namenode_port}/webhdfs/v1{encoded_hdfs_path}?op=CREATE&overwrite=true&user.name={user}"
    
    # Step 1: Initial PUT request to get redirection URL
    logger.info(f"Initiating file creation in HDFS at {hdfs_path}")
    init_response = requests.put(webhdfs_url, allow_redirects=False)
    
    if init_response.status_code != 307:
        logger.error(f"Failed to initialize HDFS file creation: {init_response.text}")
        raise Exception(f"HDFS initialization failed with status code: {init_response.status_code}")
    
    # Get the redirection URL from the Location header
    datanode_url = init_response.headers['Location']
    
    # Step 2: Download and stream to HDFS in chunks
    logger.info(f"Downloading from {download_url} and streaming to HDFS")
    with requests.get(download_url, stream=True) as download_response:
        download_response.raise_for_status()
        
        # Stream content to HDFS via the DataNode URL
        with requests.put(datanode_url, data=download_response.iter_content(chunk_size=8192)) as upload_response:
            if upload_response.status_code != 201:
                logger.error(f"Failed to write to HDFS: {upload_response.text}")
                raise Exception(f"HDFS write failed with status code: {upload_response.status_code}")
    
    logger.info(f"Successfully streamed data to HDFS at {hdfs_path}")
    return hdfs_path

if __name__ == "__main__":
    # Example usage
    download_url = "https://zenodo.org/records/1043504/files/corpus-webis-tldr-17.zip?download=1"
    hdfs_path = "/data/reddit/"
    namenode_host = "http://localhost"
    
    try:
        stream_download_to_hdfs(
            download_url=download_url,
            hdfs_path=hdfs_path,
            namenode_host=namenode_host
        )
    except Exception as e:
        logger.error(f"Error streaming data to HDFS: {str(e)}")