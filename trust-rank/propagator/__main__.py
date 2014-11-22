import pandas as pd
import numpy as np

def main():

    parser = ArgumentParser()
    parser.add_argument("--trust",
          type     = str,
          required = True,
          help     = "CSV of trust relations (can be incomplete)",
          )

    parser.add_argument("--votes",
          type     = str,
          required = True,
          help     = "CSV of initial votes on items by users",
          )

if __name__ == '__main__':
    main()
