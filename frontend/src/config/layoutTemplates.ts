import type { GridItemConfig } from "../types";

export interface LayoutTemplate {
  id: string;
  name: string;
  description: string;
  layout: GridItemConfig[];
}

// Auto-load all JSON files from the templates/ directory (eager, at build time)
const templateModules = import.meta.glob(
  './templates/*.json',
  { eager: true, import: 'default' }
) as Record<string, LayoutTemplate>;

// Build the registry from loaded JSON files
export const LAYOUT_TEMPLATES: Record<string, LayoutTemplate> = {};

for (const [, template] of Object.entries(templateModules)) {
  const t = template as LayoutTemplate;
  if (t?.id && t?.layout) {
    LAYOUT_TEMPLATES[t.id] = t;
  } else {
    console.warn('[layoutTemplates] Skipping invalid template file:', template);
  }
}

console.log(
  `[layoutTemplates] Loaded ${Object.keys(LAYOUT_TEMPLATES).length} template(s):`,
  Object.keys(LAYOUT_TEMPLATES).join(', ')
);

// Get all templates as an array for UI rendering
export const getAllTemplates = (): LayoutTemplate[] => {
  return Object.values(LAYOUT_TEMPLATES);
};

// Default template ID
export const DEFAULT_TEMPLATE_ID = "04";
