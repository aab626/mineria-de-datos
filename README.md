# mineria-de-datos

Original dataset: [Global Terrorism Database](https://www.start.umd.edu/gtd/), also [here](https://drive.google.com/file/d/1A1kh3D8OmPrzmFkv-p4HVVc7atq0Xled/view?usp=sharing) in CSV format (faster loading times), converted using:

```python
import pandas as pd
df_original = pd.read_excel('globalterrorismdb_0221dist.xlsx')
df_original.to_csv('globalterrorismdb_0221dist.csv', index=False)
```

Dataset cleaning using [this script](https://github.com/drizak/mineria-de-datos/blob/main/cleaning.py) over the CSV version, produces [this](https://drive.google.com/file/d/1tbER57wVAP7bRP_goBg_TcMTfQybU968/view?usp=sharing) as a result.

May require the `openpyxl` and `tables` libraries.
