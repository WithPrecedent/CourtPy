
import os
os.chdir('..')

from simplify import Ingredients, Inventory, Menu
from almanac import CPAlmanac

jurisdiction = 'federal'
case_type = 'appellate'
data_source = 'lexis_nexis'

menu = Menu(file_path = os.path.join('examples', 'fed_coa.ini'))
inventory = Inventory(menu = menu, root_folder = '..')
ingredients = Ingredients(menu = menu, inventory = inventory)
almanac = CPAlmanac(menu = menu,
                    inventory = inventory,
                    ingredients = ingredients,
                    data_source = data_source)
almanac.start()