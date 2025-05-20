# COMP90024 Team 61

## Project Overview
Social media give significant, real-time insights on public opinion and societal trends. Through the use of the platforms Reddit, BlueSky and Mastodon we targeted Australian public posts mentioning Trump and tariffs to analyze sentiment and overall  Australian attitude towards the Donald Trump tariff situation. This project uses cloud-native software to automate most of the pipeline, including data collection, data storage, and visualizations allowing for the analysis of large-scale social media. 
## Team Members

- Andy Chen 1353448 
- Xining Nan 1601743 
- Zhijie Guo 1511403
- Yuchen Dong 1294244
- Andy Chen 1353448

## Folder Structure

- `frontend/`: Jupyter Notebook
- `backend/`: Harvesters
- `test/`: Unit tests
- `elasticsearch/`: ElasticSearch configs and mappings
- `data/`: Data files
- `docs/`: Team report and other documents

## Setup

### How to Run the Client

1. Open **4 terminals**, and in each terminal, run the following commands to set up port forwarding:

```bash
# Terminal 1
kubectl port-forward svc/scenario3 5000:5000

# Terminal 2
kubectl port-forward svc/scenario5 5001:5000

# Terminal 3
kubectl port-forward svc/scenario4 5002:5000

# Terminal 4
kubectl port-forward service/router -n fission 9090:80
```

2. After setting up all the port forwarding, open the Jupyter notebook at `frontend/analysis.ipynb`, and **run the code blocks from top to bottom** to start the client.
## Usage

Instructions for code like python here.

## Structure
```
```