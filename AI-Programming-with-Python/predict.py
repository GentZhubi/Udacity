import argparse
import torch
import numpy as np
from PIL import Image
import json
from torchvision import models, transforms

def get_input_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str, help='Path to the image')
    parser.add_argument('checkpoint', type=str, help='Path to the model checkpoint')
    parser.add_argument('--top_k', type=int, default=5, help='Return top K predictions')
    parser.add_argument('--category_names', type=str, help='Path to the category to name json file')
    parser.add_argument('--gpu', action='store_true', help='Use GPU for inference')
    return parser.parse_args()

def load_checkpoint(filepath):
    checkpoint = torch.load(filepath)
    model = getattr(models, checkpoint['architecture'])(pretrained=True)
    model.classifier = checkpoint['classifier']
    model.load_state_dict(checkpoint['state_dict'])
    model.class_to_idx = checkpoint['class_to_idx']
    return model

def process_image(image_path):
    pil_image = Image.open(image_path)
    transform = transforms.Compose([transforms.Resize(255),
                                    transforms.CenterCrop(224),
                                    transforms.ToTensor(),
                                    transforms.Normalize([0.485, 0.456, 0.406],
                                                         [0.229, 0.224, 0.225])])
    image_tensor = transform(pil_image)
    return image_tensor.unsqueeze_(0)

def predict(image_path, model, topk=5):
    model.eval()
    with torch.no_grad():
        image_tensor = process_image(image_path).to(device)
        output = model.forward(image_tensor)
        probabilities = torch.exp(output)
        top_probs, top_classes = probabilities.topk(topk, dim=1)
        top_probs = top_probs.cpu().numpy()[0]
        idx_to_class = {v: k for k, v in model.class_to_idx.items()}
        top_classes = [idx_to_class[i] for i in top_classes.cpu().numpy()[0]]
    return top_probs, top_classes

def main():
    args = get_input_args()
    model = load_checkpoint(args.checkpoint)
    device = torch.device("cuda" if args.gpu and torch.cuda.is_available() else "cpu")
    model.to(device)
    probs, classes = predict(args.input, model, topk=args.top_k)
    if args.category_names:
        with open(args.category_names, 'r') as f:
            cat_to_name = json.load(f)
        classes = [cat_to_name[str(cls)] for cls in classes]
    print(probs)
    print(classes)

if __name__ == '__main__':
    main()
