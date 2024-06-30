import argparse
import torch
from torchvision import datasets, transforms, models

def get_input_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('data_directory', type=str, help='Path to the data directory')
    parser.add_argument('--save_dir', type=str, default='.', help='Directory to save checkpoints')
    parser.add_argument('--arch', type=str, default='vgg16', help='Model architecture')
    parser.add_argument('--learning_rate', type=float, default=0.001, help='Learning rate')
    parser.add_argument('--hidden_units', type=int, default=512, help='Number of hidden units')
    parser.add_argument('--epochs', type=int, default=1, help='Number of epochs')
    parser.add_argument('--gpu', action='store_true', help='Use GPU for training')
    return parser.parse_args()

def main():
    args = get_input_args()

    # Load the data
    train_transforms = transforms.Compose([transforms.RandomRotation(30),
                                           transforms.RandomResizedCrop(224),
                                           transforms.RandomHorizontalFlip(),
                                           transforms.ToTensor(),
                                           transforms.Normalize([0.485, 0.456, 0.406],
                                                                [0.229, 0.224, 0.225])])
    train_data = datasets.ImageFolder(args.data_directory, transform=train_transforms)
    trainloader = torch.utils.data.DataLoader(train_data, batch_size=64, shuffle=True)

    # Build the model
    model = getattr(models, args.arch)(pretrained=True)
    for param in model.parameters():
        param.requires_grad = False

    model.classifier = torch.nn.Sequential(torch.nn.Linear(25088, args.hidden_units),
                                           torch.nn.ReLU(),
                                           torch.nn.Dropout(0.2),
                                           torch.nn.Linear(args.hidden_units, 102),
                                           torch.nn.LogSoftmax(dim=1))
    criterion = torch.nn.NLLLoss()
    optimizer = torch.optim.Adam(model.classifier.parameters(), lr=args.learning_rate)
    device = torch.device("cuda" if args.gpu and torch.cuda.is_available() else "cpu")
    model.to(device)

    # Train the model
    for epoch in range(args.epochs):
        running_loss = 0
        for images, labels in trainloader:
            images, labels = images.to(device), labels.to(device)
            optimizer.zero_grad()
            log_ps = model(images)
            loss = criterion(log_ps, labels)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
        else:
            print(f"Epoch {epoch+1}/{args.epochs}.. "
                  f"Train loss: {running_loss/len(trainloader):.3f}")

    # Save checkpoint
    checkpoint = {
        'architecture': args.arch,
        'classifier': model.classifier,
        'state_dict': model.state_dict(),
        'class_to_idx': train_data.class_to_idx
    }
    torch.save(checkpoint, f"{args.save_dir}/checkpoint.pth")

if __name__ == '__main__':
    main()
