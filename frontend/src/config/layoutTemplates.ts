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

const FALLBACK_DEFAULT_TEMPLATE_ID = "04";
const envDefaultTemplateId = import.meta.env.VITE_DEFAULT_TEMPLATE_ID?.trim();

const resolveDefaultTemplateId = (): string => {
  if (envDefaultTemplateId && LAYOUT_TEMPLATES[envDefaultTemplateId]) {
    return envDefaultTemplateId;
  }

  if (envDefaultTemplateId && !LAYOUT_TEMPLATES[envDefaultTemplateId]) {
    console.warn(
      `[layoutTemplates] VITE_DEFAULT_TEMPLATE_ID='${envDefaultTemplateId}' not found. Falling back.`
    );
  }

  if (LAYOUT_TEMPLATES[FALLBACK_DEFAULT_TEMPLATE_ID]) {
    return FALLBACK_DEFAULT_TEMPLATE_ID;
  }

  return Object.keys(LAYOUT_TEMPLATES)[0] ?? FALLBACK_DEFAULT_TEMPLATE_ID;
};

// Get all templates as an array for UI rendering
export const getAllTemplates = (): LayoutTemplate[] => {
  return Object.values(LAYOUT_TEMPLATES);
};

// Default template ID
export const DEFAULT_TEMPLATE_ID = resolveDefaultTemplateId();
