import re
import os

FILES = [
    'gastronomia-local2.html',
    'gastronomia-local3.html',
    'gastronomia-local4.html',
    'gastronomia-local5.html',
    'gastronomia-independiente.html'
]

def clean_file(fname):
    with open(fname, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Clean discounts-grid (Featured)
    # <div class="discounts-grid"> ... </div>
    # We want <div class="discounts-grid"></div>
    # But wait, there might be other divs inside.
    # The content is multiline.
    
    # Regex approach is risky if nested divs exist and regex is not balanced.
    # However, standard structure is:
    # <div class="discounts-grid">
    #    <div class="product-card ..."> ... </div>
    #    ...
    # </div>
    
    # We can try to match the specific blocks I saw.
    
    # 1. discounts-grid
    # Pattern: <div class="discounts-grid">.*?</div> (non-greedy)
    # But if there are nested divs (product-card has divs), non-greedy .*? stops at first </div>.
    # This will break the HTML.
    
    # Since I know the structure, I can look for the closing tag of the grid.
    # But simpler: The grid contains product-cards.
    # I can remove all product-cards inside these grids.
    
    # <div class="discounts-grid"> -> start
    # ...
    # </div> -> end
    
    # Let's use a more robust replacement by identifying the start and the next significant tag or comment that follows the grid.
    # In discounts-grid, it is followed by button controls (prev/next) inside discounts-container?
    # No, inside discounts-container there is discounts-grid, then buttons?
    # local2.html:
    # <div class="discounts-container">
    #   <div class="discounts-grid">
    #     ... cards ...
    #   </div>
    # </div>
    
    # So I can match <div class="discounts-grid">...</div> if I can find the matching closing div.
    # Given the indentation, I can try to match by indentation? No.
    
    # Alternative: Remove all <div class="product-card ...">...</div> blocks?
    # That effectively empties the grids, but leaves whitespace.
    
    # Let's try to find the container and replace content.
    
    new_content = content
    
    # 1. discounts-grid
    # It seems to be distinct.
    # We can replace the whole discounts-grid div with an empty one.
    # Problem is identifying the end.
    
    # Let's assume the grid ends before the closing of discounts-container or the buttons.
    # In local2: </div>\s*</div>\s*<button class="discounts-nav-btn
    
    # Regex:
    # (<div class="discounts-grid">).*?(</div>\s*</div>\s*<button class="discounts-nav-btn)
    # This is tricky.
    
    # Let's use a specialized logic: 
    # Find start index of <div class="discounts-grid">
    # Count open/close divs to find the matching close.
    
    def clear_div_content(text, class_name):
        start_marker = f'<div class="{class_name}">'
        start_idx = text.find(start_marker)
        if start_idx == -1:
            print(f"Warning: {class_name} not found in {fname}")
            return text
            
        # Find matching closing div
        idx = start_idx + len(start_marker)
        open_count = 1
        
        while idx < len(text) and open_count > 0:
            if text[idx:idx+4] == '<div':
                open_count += 1
                idx += 4
            elif text[idx:idx+5] == '</div':
                open_count -= 1
                idx += 5
            else:
                idx += 1
        
        if open_count == 0:
            end_idx = idx
            # Replace content
            return text[:start_idx] + f'<div class="{class_name}"></div>' + text[end_idx:]
        else:
            print(f"Error: Could not find closing div for {class_name} in {fname}")
            return text

    new_content = clear_div_content(new_content, "discounts-grid")
    
    # 2. products-grid (Main Menu)
    # There are two products-grid. One in menu-gastronomia, one in interest-products.
    # clear_div_content only finds the first one.
    # We need to find them by context.
    
    # Helper to find by id context
    def clear_grid_in_section(text, section_id):
        # Find section start
        sec_start = text.find(f'id="{section_id}"')
        if sec_start == -1:
            # Try class for interest-products since it might not have ID or ID differs
            if section_id == 'interest-products':
                sec_start = text.find('class="products interest-products')
        
        if sec_start == -1:
             print(f"Warning: Section {section_id} not found in {fname}")
             return text
             
        # Find products-grid after sec_start
        grid_marker = '<div class="products-grid">'
        grid_start = text.find(grid_marker, sec_start)
        
        if grid_start == -1:
            print(f"Warning: products-grid not found in {section_id} in {fname}")
            return text
            
        # Find matching closing div
        idx = grid_start + len(grid_marker)
        open_count = 1
        
        while idx < len(text) and open_count > 0:
            if text[idx:idx+4] == '<div':
                open_count += 1
                idx += 4
            elif text[idx:idx+5] == '</div':
                open_count -= 1
                idx += 5
            else:
                idx += 1
                
        if open_count == 0:
            end_idx = idx
            return text[:grid_start] + '<div class="products-grid"></div>' + text[end_idx:]
        return text

    # Main Menu
    new_content = clear_grid_in_section(new_content, "menu-gastronomia")
    
    # Interest
    new_content = clear_grid_in_section(new_content, "interest-products")
    
    with open(fname, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print(f"Cleaned {fname}")

def run():
    for f in FILES:
        if os.path.exists(f):
            clean_file(f)
        else:
            print(f"File {f} not found")

if __name__ == '__main__':
    run()
